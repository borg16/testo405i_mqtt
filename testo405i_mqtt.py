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

# ---------------- Логирование ----------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("testo405i.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("Testo405i")

# ---------------- Константы BLE ----------------
UUID_WRITE = "0000fff1-0000-1000-8000-00805f9b34fb"
UUID_NOTIFY = "0000fff2-0000-1000-8000-00805f9b34fb"
UUID_SERIAL = "00002a25-0000-1000-8000-00805f9b34fb"  # Serial Number String (но у Testo часто "Serial Number" как текст)

INIT_COMMANDS = [
    "5600030000000c69023e81",
    "200000000000077b",
    "04001500000005930f0000004669726d77617265",
    "56657273696f6e304f",
    "04001500000005930f0000004669726d77617265",
    "56657273696f6e304f",
    "04001600000005d7100000004d6561737572656d",
    "656e744379636c656161",
    "110000000000035a",
]

# ---------------- Вспомогательные функции ----------------
def mqtt_server_reachable(host, port=1883, timeout=2.0):
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except Exception:
        return False


def extract_serial_from_text(text: str) -> str | None:
    """Пытаемся достать серийный номер из имени вида 'T405i SN:45866787' или похожих."""
    if not text:
        return None
    m = re.search(r'(\d{5,})', text)
    if m:
        return m.group(1)
    return None


def normalize_serial(candidate: str | None, fallback: str | None = None) -> str | None:
    """
    Приводим серийный номер к нормальному виду.
    Отбрасываем пустые значения, 'Serial Number' и т.п.
    """
    cand = (candidate or "").strip()
    if cand and cand.lower() != "serial number":
        # если в строке есть цифры — берём только их
        digits = "".join(ch for ch in cand if ch.isdigit())
        return digits or cand
    # если основной кандидат не годится — пробуем fallback
    if fallback:
        return normalize_serial(fallback, None)
    return None

# ---------------- MQTT ----------------
def mqtt_connect(cfg):
    """Подключение к MQTT (с опцией TLS)."""
    if not cfg.get("enabled", False):
        logger.info("MQTT отключён в конфигурации.")
        return None

    host = cfg.get("host")
    port = cfg.get("port", 1883)
    tls_enabled = cfg.get("tls_enabled", False)

    if not mqtt_server_reachable(host, port):
        logger.warning(f"MQTT сервер {host}:{port} недоступен. Работа без MQTT.")
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
        logger.info(f"Успешно подключено к MQTT брокеру {host}:{port}{' (TLS)' if tls_enabled else ''}.")
        return client

    except Exception as e:
        logger.error(f"Ошибка подключения к MQTT: {e}")
        return None


def mqtt_publish(client, cfg, data, serial_number: str | None = None):
    """Публикация данных в MQTT в JSON, с топиком sensors/testo405i/<serial>."""
    if not client:
        return
    base_topic = cfg.get("topic", "sensors/testo405i").rstrip("/")
    topic = f"{base_topic}/{serial_number}" if serial_number else base_topic

    payload = json.dumps(
        {k: round(v, 3) for k, v in data.items() if v is not None},
        ensure_ascii=False,
    )
    try:
        result = client.publish(
            topic,
            payload,
            qos=cfg.get("qos", 0),
            retain=cfg.get("retain", False),
        )
        if result.rc != mqtt.MQTT_ERR_SUCCESS:
            logger.warning(f"Ошибка публикации MQTT (rc={result.rc}) в топик {topic}")
        else:
            logger.debug(f"Публикация MQTT → {topic}: {payload}")
    except Exception as e:
        logger.error(f"Ошибка при публикации в MQTT: {e}")

# ---------------- CSV ----------------
class CSVLogger:
    def __init__(self, path: str):
        self.path = path
        # Создаём файл с заголовком, если его ещё нет
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

    def handle_notify(self, sender, data: bytearray):
        """Приём и парсинг BLE пакетов Testo 405i."""
        try:
            self.accum += data
            text = self.accum.decode(errors="ignore")

            if "Temperature" in text:
                self._process("Temperature")
                return

            if "Velocity" in text:
                self._process("Velocity")
                return

            # если накопили слишком много мусора — сбросим
            if len(self.accum) > 64:
                self.accum = b""

        except Exception as e:
            logger.error(f"Ошибка handle_notify: {e}")

    def _process(self, kind: str):
        """После слова Temperature/Velocity читаем следующие 8 байт и обновляем буфер."""
        try:
            word_bytes = kind.encode()
            idx = self.accum.find(word_bytes)
            if idx == -1:
                self.accum = b""
                return

            after = self.accum[idx + len(word_bytes):]
            if len(after) < 8:
                # ждём следующий фрагмент
                return

            value = struct.unpack("<f", after[0:4])[0]
            self.buffer[kind] = value

            # очищаем накопитель после успешного парса
            self.accum = b""

            self._maybe_report()

        except Exception as e:
            logger.error(f"Ошибка _process({kind}): {e}")
            self.accum = b""

    def _maybe_report(self):
        """
        Публикуем только когда есть и Temperature, и Velocity.
        И не чаще, чем раз в sample_interval секунд.
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

        logger.info(f"Измерения: Temperature: {temp:6.3f}, Velocity: {vel:6.3f}")

        if self.mqtt:
            mqtt_publish(self.mqtt, self.cfg["mqtt"], self.buffer, self.serial_number)

        if self.csv_logger:
            self.csv_logger.append(temp, vel)

# ---------------- Основная логика ----------------
async def connect_and_run(cfg):
    ble_cfg = cfg.get("ble_device", {})
    name_filter = [n.lower() for n in ble_cfg.get("name_filter", [])]
    addr = ble_cfg.get("address", "")

    device_name = None

    if addr:
        logger.info(f"Используется указанный адрес из конфигурации: {addr}")
        # Попробуем одним коротким сканом найти имя по MAC (чтобы вытащить SN из имени)
        try:
            devices = await BleakScanner.discover(timeout=3.0)
            for d in devices:
                if d.address.lower() == addr.lower():
                    device_name = d.name
                    break
            if device_name:
                logger.info(f"По MAC найдено устройство: {device_name}")
        except Exception as e:
            logger.warning(f"Не удалось выполнить предварительный скан для MAC {addr}: {e}")
    else:
        logger.info("Поиск BLE устройств (5 секунд)...")
        devices = await BleakScanner.discover(timeout=5.0, return_adv=True)
        target = None
        for dev, adv in devices.values():
            n = (dev.name or "").lower()
            logger.debug(f"Обнаружено устройство: {dev.address} | {dev.name or '<no name>'} | RSSI {adv.rssi}")
            if any(k in n for k in name_filter):
                target = dev
            elif not dev.name and adv.manufacturer_data:
                # Heuristic: Testo manufacturer IDs; при необходимости можно уточнить
                if any(mid in (0x92, 0x0d) for mid in adv.manufacturer_data.keys()):
                    target = dev

        if not target:
            raise RuntimeError("Устройство Testo 405i не найдено (укажите MAC в config.yaml).")

        addr = target.address
        device_name = target.name
        logger.info(f"Успешно найден зонд: {device_name or addr}")

    mqtt_client = mqtt_connect(cfg.get("mqtt", {}))

    csv_logger = None
    log_cfg = cfg.get("logging", {})
    if log_cfg.get("csv_logging", False):
        csv_path = log_cfg.get("csv_file", "testo405i_data.csv")
        csv_logger = CSVLogger(csv_path)
        logger.info(f"CSV логирование включено: {csv_path}")

    stop_event = asyncio.Event()

    def handle_sigint():
        logger.info("Остановка по запросу пользователя (Ctrl+C).")
        stop_event.set()

    loop = asyncio.get_running_loop()
    try:
        loop.add_signal_handler(2, handle_sigint)  # SIGINT
    except NotImplementedError:
        # На Windows add_signal_handler не поддерживается — просто игнорируем
        pass

    while not stop_event.is_set():
        try:
            async with BleakClient(addr) as client:
                if not client.is_connected:
                    await client.connect()

                # Пытаемся определить серийный номер
                serial_from_name = extract_serial_from_text(device_name)
                serial_from_gatt = None
                try:
                    data = await client.read_gatt_char(UUID_SERIAL)
                    serial_from_gatt = data.decode(errors="ignore").strip()
                except Exception:
                    pass

                serial_number = normalize_serial(serial_from_gatt, serial_from_name)
                if serial_number:
                    logger.info(f"Идентификатор устройства (серийный номер): {serial_number}")
                else:
                    logger.info("Серийный номер не определён. Публикация будет в базовый топик без суффикса.")

                logger.info(f"Успешно подключено к BLE устройству {addr}.")

                reader = TestoReader(cfg, mqtt_client, serial_number, csv_logger)
                await client.start_notify(UUID_NOTIFY, reader.handle_notify)

                # Отправляем инициализационные команды
                for h in INIT_COMMANDS:
                    await client.write_gatt_char(UUID_WRITE, bytes.fromhex(h), response=True)
                    await asyncio.sleep(0.4)

                logger.info("Начат приём данных. Нажмите Ctrl+C для завершения.")

                while client.is_connected and not stop_event.is_set():
                    await asyncio.sleep(0.5)

        except BleakError as e:
            logger.warning(f"Ошибка BLE: {e}. Повтор через 5 секунд.")
            await asyncio.sleep(5)
        except Exception as e:
            logger.error(f"Неожиданная ошибка: {e}")
            await asyncio.sleep(5)

    if mqtt_client:
        mqtt_client.loop_stop()
        mqtt_client.disconnect()

    logger.info("Работа завершена.")


def main():
    with open("config.yaml", "r") as f:
        cfg = yaml.safe_load(f)
    asyncio.run(connect_and_run(cfg))


if __name__ == "__main__":
    main()
