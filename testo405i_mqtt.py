#!/usr/bin/env python3
import asyncio
import struct
import yaml
import time
import socket
import json
import csv
import os
import re
import logging
from datetime import datetime
from bleak import BleakClient, BleakScanner
from bleak.exc import BleakError
import paho.mqtt.client as mqtt

# ---------------- Logging ----------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("testo405i.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("Testo405i")

# ---------------- BLE Constants ----------------
UUID_WRITE  = "0000fff1-0000-1000-8000-00805f9b34fb"
UUID_NOTIFY = "0000fff2-0000-1000-8000-00805f9b34fb"
UUID_SERIAL = "00002a25-0000-1000-8000-00805f9b34fb"  # Serial Number String (Testo often uses the literal text "Serial Number")

INIT_COMMANDS = [
    [b"\x56\x00\x03\x00\x00\x00", b"\x02"],
    [b"\x20\x00\x00\x00\x00\x00"],
    [b"\x04\x00\x15\x00\x00\x00", b"\x0f\x00\x00\x00FirmwareVersion"],
    [b"\x04\x00\x16\x00\x00\x00", b"\x10\x00\x00\x00MeasurementCycle"],
    [b"\x11\x00\x00\x00\x00\x00"],
]

def calc_crc16_modbus(data: bytes) -> int:
    """
    Calculate CRC-16/MODBUS for the given bytes.
    Polynomial: 0x8005 (reversed 0xA001), initial value 0xFFFF.
    Returns 16-bit CRC as integer.
    """
    crc = 0xFFFF
    for byte in data:
        crc ^= byte
        for _ in range(8):
            if crc & 0x0001:
                crc = (crc >> 1) ^ 0xA001
            else:
                crc >>= 1
    return crc


def mqtt_server_reachable(host, port=1883, timeout=2.0):
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except Exception:
        return False


def extract_serial_from_text(text: str) -> str | None:
    """Try to extract a serial number from names like 'T405i SN:45866787' or similar."""
    if not text:
        return None
    m = re.search(r'(\d{5,})', text)
    if m:
        return m.group(1)
    return None


def normalize_serial(candidate: str | None, fallback: str | None = None) -> str | None:
    """
    Normalize a serial number candidate.
    Discard empty values and placeholders like 'Serial Number'.
    """
    cand = (candidate or "").strip()
    if cand and cand.lower() != "serial number":
    # if the string contains digits — take only them
        digits = "".join(ch for ch in cand if ch.isdigit())
        return digits or cand
    # if the primary candidate is not suitable — try the fallback
    if fallback:
        return normalize_serial(fallback, None)
    return None

# ---------------- MQTT ----------------
def mqtt_connect(cfg):
    """Connect to MQTT broker (supports optional TLS) and set up Home Assistant discovery."""
    if not cfg.get("enabled", False):
        logger.info("MQTT disabled in configuration.")
        return None

    host = cfg.get("host")
    port = cfg.get("port", 1883)
    tls_enabled = cfg.get("tls_enabled", False)

    if not mqtt_server_reachable(host, port):
        logger.warning(f"MQTT server {host}:{port} unreachable. Continuing without MQTT.")
        return None

    try:
        client = mqtt.Client()
        if cfg.get("username"):
            client.username_pw_set(cfg["username"], cfg.get("password", ""))

        if tls_enabled:
            import ssl
            ca_cert = cfg.get("ca_cert")
            client_cert = cfg.get("client_cert") or None
            client_key = cfg.get("client_key") or None
            ctx = ssl.create_default_context()
            if ca_cert:
                ctx.load_verify_locations(cafile=ca_cert)
            if client_cert and client_key:
                ctx.load_cert_chain(client_cert, client_key)
            client.tls_set_context(ctx)
            client.tls_insecure_set(False)

        client.connect(host, port, 60)
        client.loop_start()
        logger.info(f"Connected to MQTT broker {host}:{port}{' (TLS)' if tls_enabled else ''}.")
        return client

    except Exception as e:
        logger.error(f"MQTT connection error: {e}")
        return None


def publish_hass_config(client, cfg, serial_number: str):
    """Publish Home Assistant MQTT discovery configuration."""
    base = cfg.get("discovery_prefix", "homeassistant")
    device_id = f"testo405i_{serial_number}" if serial_number else "testo405i"
    device_name = f"Testo 405i {serial_number}" if serial_number else "Testo 405i"
    
    device_info = {
        "identifiers": [device_id],
        "name": device_name,
        "manufacturer": "Testo",
        "model": "405i",
    }

    sensors = [
        {
            "name": "Temperature",
            "unique_id": f"{device_id}_temperature",
            "device_class": "temperature",
            "state_topic": f"testo405i/{device_id}/state",
            "unit_of_measurement": "°C",
            "value_template": "{{ value_json.Temperature }}",
            "device": device_info
        },
        {
            "name": "Velocity",
            "unique_id": f"{device_id}_velocity",
            "device_class": "speed",
            "state_topic": f"testo405i/{device_id}/state",
            "unit_of_measurement": "m/s",
            "value_template": "{{ value_json.Velocity }}",
            "device": device_info
        }
    ]

    for sensor in sensors:
        config_topic = f"{base}/sensor/{device_id}/{sensor['unique_id']}/config"
        try:
            result = client.publish(
                config_topic,
                json.dumps(sensor),
                qos=1,
                retain=True
            )
            if result.rc != mqtt.MQTT_ERR_SUCCESS:
                logger.warning(f"Failed to publish HA discovery for {sensor['name']}")
            else:
                logger.debug(f"Published HA discovery for {sensor['name']}")
        except Exception as e:
            logger.error(f"Error publishing HA discovery: {e}")

def mqtt_publish(client, cfg, data, serial_number: str | None = None):
    """Publish data to MQTT in Home Assistant compatible format."""
    if not client:
        return

    device_id = f"testo405i_{serial_number}" if serial_number else "testo405i"
    state_topic = f"testo405i/{device_id}/state"

    payload = json.dumps(
        {k: round(v, 3) for k, v in data.items() if v is not None},
        ensure_ascii=False,
    )
    try:
        result = client.publish(
            state_topic,
            payload,
            qos=cfg.get("qos", 0),
            retain=cfg.get("retain", False),
        )
        if result.rc != mqtt.MQTT_ERR_SUCCESS:
            logger.warning(f"MQTT publish error (rc={result.rc}) to topic {state_topic}")
        else:
            logger.debug(f"MQTT publish → {state_topic}: {payload}")
    except Exception as e:
        logger.error(f"Error publishing to MQTT: {e}")

# ---------------- CSV ----------------
class CSVLogger:
    def __init__(self, path: str):
        self.path = path
        # Create the file with a header if it doesn't exist yet
        if not os.path.exists(path):
            with open(path, "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(["timestamp", "temperature", "velocity"])

    def append(self, temp: float, vel: float):
        ts = datetime.now().isoformat()
        with open(self.path, "a", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([ts, f"{temp:.3f}", f"{vel:.3f}"])

# ---------------- BLE Reader ----------------
class TestoReader:
    def __init__(self, cfg, mqtt_client, serial_number: str | None, csv_logger: CSVLogger | None):
        self.cfg = cfg
        self.mqtt = mqtt_client
        self.serial_number = serial_number
        self.csv_logger = csv_logger

        self.buffer = {"Temperature": None, "Velocity": None}
        self.last_pub = 0.0
        self.accum = b""
        self.latestAnswerFuture = asyncio.Future()
        self.incompleteAnswer = b""

    async def latestAnswer(self):
        result = await asyncio.wait_for(self.latestAnswerFuture, timeout=0.4)
        self.latestAnswerFuture = asyncio.Future()
        return result

    def handle_notify(self, sender, data: bytearray):
        """Receive and parse Testo 405i BLE packets."""
        try:
            if self.incompleteAnswer:
                data = self.incompleteAnswer + data
                self.incompleteAnswer = b""
            length = data[2]
            if(length + 3 > len(data)):
                self.incompleteAnswer = data
                return

            crc = calc_crc16_modbus(data[:8])
            if crc != 0:
                logger.warning(f"CRC mismatch in received data. {hex(data[6+length]+(data[7+length]<<8))} {hex(crc)}")

            payload = data[8:8+length]
            if data.startswith(b'\x07\x00'):
                if length > 0:
                    if not self.latestAnswerFuture.done():
                        self.latestAnswerFuture.set_result(f"Command response payload: {payload[:length-2]}")
                else:
                    if not self.latestAnswerFuture.done():
                        self.latestAnswerFuture.set_result(f"Command acknowledged.")
                    return

            crc = calc_crc16_modbus(payload)
            if(crc != 0):
                logger.warning("CRC mismatch in payload.")
               
            if data.startswith(b'\x10\x80'):  # command response
                firstDataByte = 4+payload[0];
                name = (payload[4:firstDataByte]).decode('utf-8')
                # logger.info(f"Name: {name}")
                value = struct.unpack("<f",payload[firstDataByte:firstDataByte+4])[0]
                # logger.info(f"Value: {value}")
                self.buffer[name] = value
                self._maybe_report()

        except Exception as e:
            logger.error(f"handle_notify error: {e}")


    def _maybe_report(self):
        """
        Publish only when both Temperature and Velocity are present.
        Also respect the configured sample_interval to avoid publishing too often.
        """
        if self.buffer["Temperature"] is None or self.buffer["Velocity"] is None:
            return

        now = time.time()
        interval = float(self.cfg.get("logging", {}).get("sample_interval", 5.0))
        if now - self.last_pub < interval:
            return

        self.last_pub = now
        temp = self.buffer["Temperature"]
        vel = self.buffer["Velocity"]

        logger.debug(f"Measurements: Temperature: {temp:6.3f}, Velocity: {vel:6.3f}")

        if self.mqtt:
            mqtt_publish(self.mqtt, self.cfg["mqtt"], self.buffer, self.serial_number)

        if self.csv_logger:
            self.csv_logger.append(temp, vel)

# ---------------- Main logic ----------------
async def connect_and_run(cfg):
    ble_cfg = cfg.get("ble_device", {})
    name_filter = [n.lower() for n in ble_cfg.get("name_filter", [])]
    addr = ble_cfg.get("address", "")

    device_name = None

    if addr:
        logger.info(f"Using configured address: {addr}")
        # Try a short scan to find the device name by MAC (to extract SN from the name)
        try:
            devices = await BleakScanner.discover(timeout=3.0)
            for d in devices:
                if d.address.lower() == addr.lower():
                    device_name = d.name
                    break
            if device_name:
                logger.info(f"Device found by MAC: {device_name}")
        except Exception as e:
            logger.warning(f"Pre-scan for MAC {addr} failed: {e}")
    else:
        logger.info("Scanning for BLE devices (5 seconds)...")
        devices = await BleakScanner.discover(timeout=5.0, return_adv=True)
        target = None
        for dev, adv in devices.values():
            n = (dev.name or "").lower()
            logger.debug(f"Discovered device: {dev.address} | {dev.name or '<no name>'} | RSSI {adv.rssi}")
            if any(k in n for k in name_filter):
                target = dev
            elif not dev.name and adv.manufacturer_data:
                # Heuristic: Testo manufacturer IDs; can be refined if needed
                if any(mid in (0x92, 0x0d) for mid in adv.manufacturer_data.keys()):
                    target = dev

        if not target:
            raise RuntimeError("Testo 405i device not found (specify MAC in config.yaml).")

        addr = target.address
        device_name = target.name
    logger.info(f"Probe found: {device_name or addr}")

    mqtt_client = mqtt_connect(cfg.get("mqtt", {}))

    csv_logger = None
    log_cfg = cfg.get("logging", {})
    csv_path = log_cfg.get("csv_file", "testo405i_data.csv")
    if log_cfg.get("csv_logging", False):
        csv_logger = CSVLogger(csv_path)
    logger.info(f"CSV logging enabled: {csv_path}")

    stop_event = asyncio.Event()

    def handle_sigint():
        logger.info("Stopping on user request (Ctrl+C).")
        stop_event.set()

    loop = asyncio.get_running_loop()
    try:
        loop.add_signal_handler(2, handle_sigint)  # SIGINT
    except NotImplementedError:
        # On some platforms (like Windows) add_signal_handler isn't supported — ignore
        pass

    while not stop_event.is_set():
        try:
            async with BleakClient(addr) as client:
                if not client.is_connected:
                    await client.connect()

                # Try to determine the serial number
                serial_from_name = extract_serial_from_text(device_name)
                serial_from_gatt = None
                try:
                    data = await client.read_gatt_char(UUID_SERIAL)
                    serial_from_gatt = data.decode(errors="ignore").strip()
                except Exception:
                    pass

                serial_number = normalize_serial(serial_from_gatt, serial_from_name)
                if serial_number:
                    logger.info(f"Device identifier (serial number): {serial_number}")
                else:
                    logger.info("Serial number not determined. Publishing to base topic without suffix.")

                logger.info(f"Successfully connected to BLE device {addr}.")
                
                # Publish Home Assistant MQTT discovery configuration if MQTT is enabled
                if mqtt_client:
                    publish_hass_config(mqtt_client, cfg.get("mqtt", {}), serial_number)

                reader = TestoReader(cfg, mqtt_client, serial_number, csv_logger)
                await client.start_notify(UUID_NOTIFY, reader.handle_notify)

                # Send initialization commands
                for h in INIT_COMMANDS:
                    logger.info(f"Sending init command: {b''.join(h)}")
                    for subcommand in h:
                        await sendcommand(client, subcommand)
                    answer = await reader.latestAnswer()
                    logger.info(f"Init command response: {answer}")

                logger.info("Started receiving data. Press Ctrl+C to stop.")

                while client.is_connected and not stop_event.is_set():
                    await asyncio.sleep(0.5)

        except BleakError as e:
            logger.warning(f"BLE error: {e}. Retrying in 5 seconds.")
            await asyncio.sleep(5)
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            await asyncio.sleep(5)

    if mqtt_client:
        mqtt_client.loop_stop()
        mqtt_client.disconnect()

    logger.info("Shutdown complete.")

async def sendcommand(client: BleakClient, h: bytes):
    max_chunk = 18
    crc = calc_crc16_modbus(h)
    crc_bytes = bytes([crc & 0xFF, (crc >> 8) & 0xFF])

    if len(h) <= max_chunk:
        command = h + crc_bytes
        logger.debug(f"Sending init command: {command}")
        await client.write_gatt_char(UUID_WRITE, command, response=True)
    else:
        # send in multiple chunks, append CRC only to the last chunk
        chunks = [h[i:i+max_chunk] for i in range(0, len(h), max_chunk)]
        total = len(chunks)
        for i, chunk in enumerate(chunks):
            if i == total - 1:
                to_send = chunk + crc_bytes
            else:
                to_send = chunk
            logger.debug(f"Sending init command chunk {i+1}/{total}: {to_send}")
            await client.write_gatt_char(UUID_WRITE, to_send, response=True)

def main():
    with open("config.yaml", "r") as f:
        cfg = yaml.safe_load(f)
    asyncio.run(connect_and_run(cfg))


if __name__ == "__main__":
    main()
