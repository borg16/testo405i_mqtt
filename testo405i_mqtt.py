#!/usr/bin/env python3
"""
Testo 405i BLE to MQTT Bridge

Connects to Testo 405i temperature/airflow probe via Bluetooth LE,
reads measurements, and publishes to MQTT with Home Assistant auto-discovery.
"""

import asyncio
import csv
import json
import logging
import os
import re
import socket
import struct
import time
from datetime import datetime

import yaml
from bleak import BleakClient, BleakScanner
from bleak.exc import BleakError
import paho.mqtt.client as mqtt


# ============================================================================
# Logging Configuration
# ============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("testo405i.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("Testo405i")


# ============================================================================
# BLE Constants
# ============================================================================

UUID_WRITE = "0000fff1-0000-1000-8000-00805f9b34fb"
UUID_NOTIFY = "0000fff2-0000-1000-8000-00805f9b34fb"
UUID_SERIAL = "00002a25-0000-1000-8000-00805f9b34fb"

# Initialization command sequences sent to Testo 405i during setup
INIT_COMMANDS = [
    [b"\x56\x00\x03\x00\x00\x00", b"\x02"],
    [b"\x20\x00\x00\x00\x00\x00"],
    [b"\x04\x00\x15\x00\x00\x00", b"\x0f\x00\x00\x00FirmwareVersion"],
    [b"\x04\x00\x16\x00\x00\x00", b"\x10\x00\x00\x00MeasurementCycle"],
    [b"\x11\x00\x00\x00\x00\x00"],
]


# ============================================================================
# CRC Calculation
# ============================================================================

def calc_crc16_modbus(data: bytes) -> int:
    """
    Calculate CRC-16/MODBUS for the given bytes.
    
    Args:
        data: Input bytes to calculate CRC for
        
    Returns:
        16-bit CRC value
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


# ============================================================================
# Utility Functions
# ============================================================================

def mqtt_server_reachable(host: str, port: int = 1883, timeout: float = 2.0) -> bool:
    """
    Check if MQTT broker is reachable.
    
    Args:
        host: MQTT broker hostname
        port: MQTT broker port
        timeout: Connection timeout in seconds
        
    Returns:
        True if broker is reachable, False otherwise
    """
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except Exception:
        return False


def extract_serial_from_text(text: str) -> str | None:
    """
    Extract serial number from device name or text.
    
    Args:
        text: Text to search for serial number (e.g., 'T405i SN:45866787')
        
    Returns:
        Serial number if found, None otherwise
    """
    if not text:
        return None
    match = re.search(r'(\d{5,})', text)
    return match.group(1) if match else None


def normalize_serial(candidate: str | None, fallback: str | None = None) -> str | None:
    """
    Normalize and validate a serial number candidate.
    
    Args:
        candidate: Primary serial number candidate
        fallback: Fallback serial number if primary is invalid
        
    Returns:
        Normalized serial number or None
    """
    cand = (candidate or "").strip()
    if cand and cand.lower() != "serial number":
        # Extract only digits if present
        digits = "".join(ch for ch in cand if ch.isdigit())
        return digits or cand
    
    # Try fallback if primary candidate is invalid
    if fallback:
        return normalize_serial(fallback, None)
    return None



# ============================================================================
# MQTT Functions
# ============================================================================

def mqtt_connect(cfg: dict):
    """
    Connect to MQTT broker with optional TLS support.
    
    Args:
        cfg: MQTT configuration dictionary
        
    Returns:
        MQTT client instance or None if connection failed/disabled
    """
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
        client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        
        # Set authentication if provided
        if cfg.get("username"):
            client.username_pw_set(cfg["username"], cfg.get("password", ""))

        # Configure TLS if enabled
        if tls_enabled:
            import ssl
            ca_cert = cfg.get("ca_cert")
            client_cert = cfg.get("client_cert")
            client_key = cfg.get("client_key")
            
            ctx = ssl.create_default_context()
            if ca_cert:
                ctx.load_verify_locations(cafile=ca_cert)
            if client_cert and client_key:
                ctx.load_cert_chain(client_cert, client_key)
            client.tls_set_context(ctx)
            client.tls_insecure_set(False)

        client.connect(host, port, 60)
        client.loop_start()
        
        tls_str = " (TLS)" if tls_enabled else ""
        logger.info(f"Connected to MQTT broker {host}:{port}{tls_str}")
        return client

    except ssl.SSLError as e:
        logger.error(f"MQTT SSL error: {e}. Check your TLS/SSL certificates and configuration.")
        return None
    except socket.error as e:
        logger.error(f"MQTT network error: {e}. Check your network connectivity and broker address.")
        return None
    except mqtt.MQTTException as e:
        logger.error(f"MQTT library error: {e}. Check your MQTT client configuration.")
        return None
    except Exception as e:
        logger.error(f"Unexpected MQTT connection error: {e}")
        return None


def publish_hass_config(client, cfg: dict, serial_number: str | None):
    """
    Publish Home Assistant MQTT discovery configuration.
    
    Args:
        client: MQTT client instance
        cfg: MQTT configuration dictionary
        serial_number: Device serial number (or None)
    """
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
            result = client.publish(config_topic, json.dumps(sensor), qos=1, retain=True)
            if result.rc != mqtt.MQTT_ERR_SUCCESS:
                logger.warning(f"Failed to publish HA discovery for {sensor['name']}")
            else:
                logger.debug(f"Published HA discovery for {sensor['name']}")
        except Exception as e:
            logger.error(f"Error publishing HA discovery: {e}")


def mqtt_publish(client, cfg: dict, data: dict, serial_number: str | None = None):
    """
    Publish measurement data to MQTT in Home Assistant compatible format.
    
    Args:
        client: MQTT client instance
        cfg: MQTT configuration dictionary
        data: Measurement data dictionary
        serial_number: Device serial number (or None)
    """
    if not client:
        return

    device_id = f"testo405i_{serial_number}" if serial_number else "testo405i"
    state_topic = f"testo405i/{device_id}/state"

    # Round values to 3 decimal places and exclude None values
    payload = json.dumps(
        {k: round(v, 3) for k, v in data.items() if v is not None},
        ensure_ascii=False
    )
    
    try:
        result = client.publish(
            state_topic,
            payload,
            qos=cfg.get("qos", 0),
            retain=cfg.get("retain", False)
        )
        if result.rc != mqtt.MQTT_ERR_SUCCESS:
            logger.warning(f"MQTT publish error (rc={result.rc}) to topic {state_topic}")
        else:
            logger.debug(f"MQTT publish → {state_topic}: {payload}")
    except Exception as e:
        logger.error(f"Error publishing to MQTT: {e}")



# ============================================================================
# CSV Logging
# ============================================================================

class CSVLogger:
    """CSV logger for recording temperature and velocity measurements."""
    
    def __init__(self, path: str):
        """
        Initialize CSV logger.
        
        Args:
            path: Path to CSV file
        """
        self.path = path
        
        # Create file with header if it doesn't exist
        if not os.path.exists(path):
            with open(path, "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(["timestamp", "temperature", "velocity"])

    def append(self, temp: float, vel: float):
        """
        Append measurement to CSV file.
        
        Args:
            temp: Temperature value
            vel: Velocity value
        """
        timestamp = datetime.now().isoformat()
        with open(self.path, "a", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([timestamp, f"{temp:.3f}", f"{vel:.3f}"])



# ============================================================================
# BLE Reader - Handles Testo 405i Communication
# ============================================================================

class TestoReader:
    """Handles BLE notifications and data parsing from Testo 405i device."""
    
    def __init__(self, cfg: dict, mqtt_client, serial_number: str | None, 
                 csv_logger: CSVLogger | None):
        """
        Initialize Testo reader.
        
        Args:
            cfg: Configuration dictionary
            mqtt_client: MQTT client instance
            serial_number: Device serial number
            csv_logger: CSV logger instance
        """
        self.cfg = cfg
        self.mqtt = mqtt_client
        self.serial_number = serial_number
        self.csv_logger = csv_logger
        
        # Accumulate sums and counts for averaging
        self.sums = {"Temperature": 0.0, "Velocity": 0.0}
        self.counts = {"Temperature": 0, "Velocity": 0}
        self.last_pub = 0.0
        
        # BLE communication state
        self.accum = b""
        self.latestAnswerFuture = asyncio.Future()
        self.incompleteAnswer = b""

    async def latestAnswer(self) -> str:
        """
        Wait for and return the latest command response.
        
        Returns:
            Response string from device
        """
        result = await asyncio.wait_for(self.latestAnswerFuture, timeout=0.4)
        self.latestAnswerFuture = asyncio.Future()
        return result

    def handle_notify(self, characteristic, data: bytearray):
        """
        Handle BLE notification from Testo 405i device.

        Args:
            characteristic: BleakGATTCharacteristic that sent the notification
            data: Raw data bytes from device
        """
        try:
            # Reassemble fragmented packets
            if self.incompleteAnswer:
                data = self.incompleteAnswer + data
                self.incompleteAnswer = b""

            # Check if we have complete packet
            length = data[2]
            if length + 3 > len(data):
                self.incompleteAnswer = data
                return

            # Verify CRC of header
            crc = calc_crc16_modbus(data[:8])
            if crc != 0:
                logger.warning(
                    f"CRC mismatch in received data. "
                    f"Expected {hex(data[6+length]+(data[7+length]<<8))}, got {hex(crc)}"
                )

            # Extract payload
            payload = data[8:8+length]

            # Handle command acknowledgment
            if data.startswith(b'\x07\x00'):
                if length > 0:
                    if not self.latestAnswerFuture.done():
                        self.latestAnswerFuture.set_result(
                            f"Command response payload: {payload[:length-2]}"
                        )
                else:
                    if not self.latestAnswerFuture.done():
                        self.latestAnswerFuture.set_result("Command acknowledged.")
                return

            # Verify payload CRC
            crc = calc_crc16_modbus(payload)
            if crc != 0:
                logger.warning("CRC mismatch in payload - dropping data.")
                return

            # Handle measurement data
            if data.startswith(b'\x10\x80'):
                first_data_byte = 4 + payload[0]
                name = payload[4:first_data_byte].decode('utf-8')
                value = struct.unpack("<f", payload[first_data_byte:first_data_byte+4])[0]

                # Accumulate value for averaging
                if name in self.sums:
                    self.sums[name] += value
                    self.counts[name] += 1
                    self._maybe_report()

        except Exception as e:
            logger.error(f"Unexpected notify {type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}: {e}")

    def _maybe_report(self):
        """
        Publish averaged measurements if both metrics available and interval elapsed.
        """
        # Require at least one sample for each metric
        if self.counts["Temperature"] == 0 or self.counts["Velocity"] == 0:
            return

        # Check sample interval
        now = time.time()
        interval = float(self.cfg.get("logging", {}).get("sample_interval", 5.0))
        if now - self.last_pub < interval:
            return

        # Compute averages
        temp = self.sums["Temperature"] / self.counts["Temperature"]
        vel = self.sums["Velocity"] / self.counts["Velocity"]

        # Reset accumulators for next averaging window
        self.sums = {"Temperature": 0.0, "Velocity": 0.0}
        self.counts = {"Temperature": 0, "Velocity": 0}
        self.last_pub = now

        logger.debug(f"Measurements (avg): Temperature: {temp:6.3f}, Velocity: {vel:6.3f}")

        # Publish to MQTT
        if self.mqtt:
            mqtt_publish(
                self.mqtt,
                self.cfg["mqtt"],
                {"Temperature": temp, "Velocity": vel},
                self.serial_number
            )

        # Log to CSV
        if self.csv_logger:
            self.csv_logger.append(temp, vel)



# ============================================================================
# BLE Command Functions
# ============================================================================

async def send_command(client: BleakClient, command: bytes):
    """
    Send command to Testo 405i device with CRC.
    
    Commands longer than 18 bytes are chunked, with CRC appended to final chunk.
    
    Args:
        client: BLE client instance
        command: Command bytes to send
    """
    max_chunk = 18
    crc = calc_crc16_modbus(command)
    crc_bytes = bytes([crc & 0xFF, (crc >> 8) & 0xFF])

    if len(command) <= max_chunk:
        # Send in single packet
        full_command = command + crc_bytes
        logger.debug(f"Sending command: {full_command.hex()}")
        await client.write_gatt_char(UUID_WRITE, full_command, response=True)
    else:
        # Send in multiple chunks, append CRC only to last chunk
        chunks = [command[i:i+max_chunk] for i in range(0, len(command), max_chunk)]
        total = len(chunks)
        
        for i, chunk in enumerate(chunks):
            to_send = chunk + crc_bytes if i == total - 1 else chunk
            logger.debug(f"Sending command chunk {i+1}/{total}: {to_send.hex()}")
            await client.write_gatt_char(UUID_WRITE, to_send, response=True)


# ============================================================================
# Main Connection Logic
# ============================================================================

async def connect_and_run(cfg: dict):
    """
    Main connection and data collection loop.
    
    Args:
        cfg: Configuration dictionary from config.yaml
    """
    # Extract BLE configuration
    ble_cfg = cfg.get("ble_device", {})
    name_filter = [n.lower() for n in ble_cfg.get("name_filter", [])]
    addr = ble_cfg.get("address", "")
    adapter = ble_cfg.get("adapter", "hci0")
    
    device_name = None

    # Device discovery or direct connection
    if addr:
        logger.info(f"Using configured address: {addr}")
        # Try to discover device name from address
        try:
            devices = await BleakScanner.discover(timeout=3.0)
            for device in devices:
                if device.address.lower() == addr.lower():
                    device_name = device.name
                    break
            if device_name:
                logger.info(f"Device found by MAC: {device_name}")
        except Exception as e:
            logger.warning(f"Pre-scan for MAC {addr} failed: {e}")
    else:
        # Scan for devices matching filter
        logger.info("Scanning for BLE devices (5 seconds)...")
        devices = await BleakScanner.discover(timeout=5.0, return_adv=True)
        target = None
        
        for dev, adv in devices.values():
            device_name_lower = (dev.name or "").lower()
            logger.debug(
                f"Discovered: {dev.address} | {dev.name or '<no name>'} | RSSI {adv.rssi}"
            )
            
            # Check name filter
            if any(keyword in device_name_lower for keyword in name_filter):
                target = dev
                break
            
            # Check manufacturer data (Testo IDs: 0x92, 0x0d)
            if not dev.name and adv.manufacturer_data:
                if any(mid in (0x92, 0x0d) for mid in adv.manufacturer_data.keys()):
                    target = dev
                    break

        if not target:
            raise RuntimeError("Testo 405i device not found. Specify MAC in config.yaml.")

        addr = target.address
        device_name = target.name
        
    logger.info(f"Probe found: {device_name or addr}")

    # Initialize MQTT and CSV logging
    mqtt_client = mqtt_connect(cfg.get("mqtt", {}))

    csv_logger = None
    log_cfg = cfg.get("logging", {})
    if log_cfg.get("csv_logging", False):
        csv_path = log_cfg.get("csv_file", "testo405i_data.csv")
        csv_logger = CSVLogger(csv_path)
        logger.info(f"CSV logging enabled: {csv_path}")

    # Set up signal handling for graceful shutdown
    stop_event = asyncio.Event()

    def handle_sigint():
        logger.info("Stopping on user request (Ctrl+C)")
        stop_event.set()

    loop = asyncio.get_running_loop()
    try:
        loop.add_signal_handler(2, handle_sigint)  # SIGINT
    except NotImplementedError:
        # Signal handlers not supported on Windows
        pass

    # Main connection loop with reconnection on errors
    while not stop_event.is_set():
        try:
            # Increase timeout for BlueZ 5.82+ compatibility
            async with BleakClient(
                addr, 
                adapter=adapter,
                timeout=30.0  # Longer timeout for connection establishment
            ) as client:

                # Determine serial number
                serial_from_name = extract_serial_from_text(device_name)
                serial_from_gatt = None
                
                try:
                    data = await client.read_gatt_char(UUID_SERIAL)
                    serial_from_gatt = data.decode(errors="ignore").strip()
                except Exception:
                    pass

                serial_number = normalize_serial(serial_from_gatt, serial_from_name)
                if serial_number:
                    logger.info(f"Device serial number: {serial_number}")
                else:
                    logger.error("Serial number not determined - cannot identify device. Reconnecting...")
                    await asyncio.sleep(5)
                    continue

                logger.info(f"Connected to BLE device {addr}")
                
                # Publish Home Assistant MQTT discovery
                if mqtt_client:
                    publish_hass_config(mqtt_client, cfg.get("mqtt", {}), serial_number)

                # Initialize reader and start notifications
                reader = TestoReader(cfg, mqtt_client, serial_number, csv_logger)
                await client.start_notify(UUID_NOTIFY, reader.handle_notify)

                # Send initialization commands
                for command_sequence in INIT_COMMANDS:
                    logger.info(f"Sending init command: {b''.join(command_sequence).hex()}")
                    for subcommand in command_sequence:
                        await send_command(client, subcommand)
                    answer = await reader.latestAnswer()
                    logger.info(f"Init response: {answer}")

                logger.info("Started receiving data. Press Ctrl+C to stop.")

                # Keep connection alive with aggressive periodic activity
                # BlueZ 5.82+ has stricter supervision timeout enforcement
                last_keepalive = time.time()
                keepalive_interval = 30.0  # Aggressive 5-second keep-alive for new BlueZ
                keepalive_counter = 0
                
                while client.is_connected and not stop_event.is_set():
                    await asyncio.sleep(0.5)
                    
                    # Perform periodic activity to maintain connection
                    now = time.time()
                    if now - last_keepalive >= keepalive_interval:
                        try:
                            # Alternate between reading characteristics to maintain traffic
                            if keepalive_counter % 2 == 0:
                                # Read serial number
                                await client.read_gatt_char(UUID_SERIAL)
                            else:
                                # Re-subscribe to notifications (no-op if already subscribed)
                                # This generates connection activity without disrupting data
                                await client.start_notify(UUID_NOTIFY, reader.handle_notify)
                            
                            logger.debug(f"Keep-alive activity {keepalive_counter}")
                            last_keepalive = now
                            keepalive_counter += 1
                        except Exception as e:
                            logger.warning(f"Keep-alive failed: {type(e).__name__}")
                            # Connection likely dead, let main loop detect it
                
                if not client.is_connected:
                    logger.warning("BLE device disconnected. Reconnecting in 5s...")
                    await asyncio.sleep(5)

        except BleakError as e:
            logger.warning(f"BLE error: {e}. Retrying in 5 seconds.")
            await asyncio.sleep(5)
        except Exception as e:
            logger.error(f"Unexpected {type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}: {e}")
            await asyncio.sleep(5)

    # Cleanup
    if mqtt_client:
        mqtt_client.loop_stop()
        mqtt_client.disconnect()

    logger.info("Shutdown complete")


def main():
    """Load configuration and start the application."""
    try:
        with open("config.yaml", "r") as f:
            cfg = yaml.safe_load(f)
        asyncio.run(connect_and_run(cfg))
    except FileNotFoundError:
        logger.error("config.yaml not found. Please create it from config_example.yaml")
    except Exception as e:
        logger.error(f"Fatal error: {e}")


if __name__ == "__main__":
    main()
