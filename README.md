# Testo 405i BLE → MQTT bridge

Приложение на Python для чтения данных с анемометра **Testo 405i** по Bluetooth Low Energy (BLE)  
и публикации температуры и скорости воздушного потока в **MQTT** (в JSON-формате).  
Дополнительно может сохранять измерения в CSV-файл и вести подробный лог.

---

## 🇷🇺 Описание (на русском)

### Возможности
- Автоматический поиск или подключение по MAC-адресу BLE-зонда Testo 405i  
- Декодирование измерений **Temperature** и **Velocity**
- Отправка данных в MQTT-брокер (с поддержкой **TLS/SSL**)
- Формат сообщений — JSON  
  ```json
  {
    "Temperature": 23.815,
    "Velocity": 0.021
  }
  ```
- Автоматическое добавление **серийного номера** устройства в топик:  
  `sensors/testo405i/<serial_number>`
- Опциональное логирование измерений в CSV
- Подробный лог работы в `testo405i.log`
- Устойчивость к обрывам BLE-связи — автоматическое переподключение

---

### Установка
```bash
sudo apt install python3-pip python3-venv bluetooth libbluetooth-dev
python3 -m venv venv
source venv/bin/activate
pip install bleak paho-mqtt PyYAML
```

*(Рекомендуется использовать виртуальное окружение, особенно на системах с установленными пакетами BLE).*

---

### Конфигурация

Создайте файл **`config.yaml`** рядом со скриптом:

```yaml
ble_device:
  name_filter: ["testo", "t405", "405i"]
  address: "A4:06:E9:86:07:4D"   # Можно оставить пустым, чтобы выполнялся поиск

mqtt:
  enabled: true
  host: "mqtt.server.com"
  port: 8883
  username: "user"
  password: "pass"
  topic: "sensors/testo405i"
  qos: 1
  retain: false
  tls_enabled: true
  ca_cert: "/etc/ssl/certs/ca-certificates.crt"

logging:
  sample_interval: 5.0          # Интервал публикации, сек
  console_output: true
  csv_logging: true
  csv_file: "testo405i_data.csv"
```

---

### Запуск

```bash
python3 testo405i_mqtt.py
```

После успешного подключения появятся строки вида:

```
2025-11-07 16:30:55,334 [INFO] Устройство Testo 405i, серийный номер: 45866787
2025-11-07 16:30:58,321 [INFO] Начат приём данных.
2025-11-07 16:31:03,770 [INFO] Измерения: Temperature: 23.815, Velocity: 0.020
```

---

### MQTT-сообщения

Топик:  
```
sensors/testo405i/45866787
```

Сообщение (JSON):  
```json
{"Temperature": 23.815, "Velocity": 0.020}
```

---

### CSV-логирование

При включении в конфиге (`csv_logging: true`) создаётся файл:

```
timestamp,temperature,velocity
2025-11-07T16:31:03.770,23.815,0.020
2025-11-07T16:31:09.718,23.818,0.015
```

---

### Завершение работы

Остановить можно комбинацией **Ctrl + C**.  
Скрипт корректно завершает BLE-и MQTT-соединения.

---

### Совместимость

| Компонент | Версия |
|------------|---------|
| Python | ≥ 3.9 |
| Bleak | ≥ 0.22 |
| paho-mqtt | ≥ 1.6 |
| PyYAML | ≥ 6.0 |

---

## 🇬🇧 English Description

### Overview
A Python utility that connects to the **Testo 405i** anemometer via Bluetooth Low Energy (BLE),  
reads **temperature** and **air velocity**, and publishes them to an **MQTT broker** in JSON format.  
Optionally logs all measurements to CSV and to a rotating log file.

---

### Features
- Automatic BLE discovery or manual MAC-based connection  
- Decodes *Temperature* and *Velocity* characteristics  
- Publishes data to MQTT (supports **TLS/SSL**)  
- JSON payload:
  ```json
  {
    "Temperature": 23.81,
    "Velocity": 0.02
  }
  ```
- Topic suffix includes the device **serial number**:  
  `sensors/testo405i/<serial_number>`
- Optional CSV data logging  
- Robust BLE reconnect handling  
- Clean logging and structured output

---

### Installation
```bash
sudo apt install python3-pip python3-venv bluetooth libbluetooth-dev
python3 -m venv venv
source venv/bin/activate
pip install bleak paho-mqtt PyYAML
```

---

### Configuration

Create **`config.yaml`** in the same directory:

```yaml
ble_device:
  name_filter: ["testo", "t405", "405i"]
  address: ""                      # Leave empty to auto-discover

mqtt:
  enabled: true
  host: "mqtt.server.com"
  port: 8883
  username: "user"
  password: "pass"
  topic: "sensors/testo405i"
  qos: 1
  retain: false
  tls_enabled: true
  ca_cert: "/etc/ssl/certs/ca-certificates.crt"

logging:
  sample_interval: 5.0
  console_output: true
  csv_logging: true
  csv_file: "testo405i_data.csv"
```

---

### Running
```bash
python3 testo405i_mqtt.py
```

Sample output:
```
2025-11-07 16:30:55,334 [INFO] Device Testo 405i, serial number: 45866787
2025-11-07 16:30:58,321 [INFO] Data acquisition started.
2025-11-07 16:31:03,770 [INFO] Measurement: Temperature: 23.815, Velocity: 0.020
```

---

### MQTT Message

Topic:
```
sensors/testo405i/45866787
```

Payload:
```json
{"Temperature": 23.815, "Velocity": 0.020}
```

---

### CSV Logging

If enabled (`csv_logging: true`), data are stored in:

```
timestamp,temperature,velocity
2025-11-07T16:31:03.770,23.815,0.020
2025-11-07T16:31:09.718,23.818,0.015
```

---

### Stopping
Press **Ctrl + C** to terminate gracefully.  
All BLE and MQTT connections are closed cleanly.

---

### Requirements

| Component | Version |
|------------|----------|
| Python | ≥ 3.9 |
| Bleak | ≥ 0.22 |
| paho-mqtt | ≥ 1.6 |
| PyYAML | ≥ 6.0 |

---

### Author
Developed for automation of Testo 405i BLE measurements and integration into MQTT-based monitoring systems.
