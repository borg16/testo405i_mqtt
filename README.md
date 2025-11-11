# Testo 405i MQTT Bridge

## Overview
A Python utility that connects to the **Testo 405i** anemometer via Bluetooth Low Energy (BLE),  
reads **temperature** and **air velocity**, and publishes averaged measurements to an **MQTT broker**.  
Includes **Home Assistant auto-discovery** support, optional CSV logging, and systemd integration.

---

## Features
- **Automatic BLE discovery** or manual MAC-based connection  
- **Configurable BLE adapter** (hci0, hci1, etc.)
- **Home Assistant MQTT Discovery** - sensors appear automatically in Home Assistant
- **Averaging** - publishes average values over configurable intervals
- Decodes *Temperature* (°C) and *Velocity* (m/s) measurements  
- Publishes data to MQTT with **TLS/SSL support**
- Device-specific topics using serial number
- Optional **CSV data logging**  
- Robust **BLE reconnect** handling  
- **systemd service** support for running as a daemon
- Clean logging with configurable verbosity

---

## Installation

### System Dependencies
```bash
sudo apt install python3-pip python3-venv bluetooth libbluetooth-dev
```

### Python Environment
```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

Or install packages individually:
```bash
pip install bleak paho-mqtt PyYAML
```

---

## Configuration

Create **`config.yaml`** from the example:

```bash
cp config_example.yaml config.yaml
nano config.yaml
```

### Configuration Options

```yaml
ble_device:
  # Optional: Specify MAC address for direct connection
  address: "A4:06:E9:8E:3D:D9"
  
  # Optional: BLE adapter to use (default: hci0)
  adapter: "hci0"
  
  # Device name filters for auto-discovery
  name_filter:
    - "405i"
    - "testo"

mqtt:
  enabled: true
  host: "homeassistant.local"
  port: 1883
  username: "mqtt_user"
  password: "mqtt_password"
  
  # Home Assistant MQTT Discovery prefix
  discovery_prefix: "homeassistant"
  
  # MQTT Quality of Service (0, 1, or 2)
  qos: 0
  retain: false
  
  # Optional: TLS configuration
  tls_enabled: false
  ca_cert: "/etc/ssl/certs/ca-certificates.crt"
  # client_cert: "client.crt"
  # client_key: "client.key"

logging:
  # Averaging interval in seconds
  sample_interval: 5.0
  
  # CSV logging
  csv_logging: true
  csv_file: "testo405i_data.csv"
```

---

## Running

### Manual Execution
```bash
python3 testo405i_mqtt.py
```

### As a systemd Service

1. Create the service file `/etc/systemd/system/testo405i-mqtt.service`:

```ini
[Unit]
Description=Testo 405i MQTT Bridge
After=network-online.target bluetooth.target
Wants=network-online.target

[Service]
Type=simple
User=<user>
Group=<user>
WorkingDirectory=/usr/local/lib/testo405i_mqtt/
ExecStart=/usr/bin/python3 /usr/local/lib/testo405i_mqtt/testo405i_mqtt.py
Restart=on-failure
RestartSec=10
StandardOutput=journal
StandardError=journal

# Bluetooth capabilities
AmbientCapabilities=CAP_NET_RAW CAP_NET_ADMIN
CapabilityBoundingSet=CAP_NET_RAW CAP_NET_ADMIN

[Install]
WantedBy=multi-user.target
```

2. Enable and start the service:

```bash
sudo systemctl daemon-reload
sudo systemctl enable testo405i-mqtt
sudo systemctl start testo405i-mqtt
```

3. Control the service:

```bash
# Check status
sudo systemctl status testo405i-mqtt

# View logs
sudo journalctl -u testo405i-mqtt -f

# Restart
sudo systemctl restart testo405i-mqtt

# Stop
sudo systemctl stop testo405i-mqtt
```

---

## MQTT Topics

### State Topic
Published measurements use device-specific topics:
```
testo405i/testo405i_45823109/state
```

Payload (JSON):
```json
{
  "Temperature": 21.073,
  "Velocity": 1.635
}
```

### Home Assistant Discovery
Auto-discovery configuration is published to:
```
homeassistant/sensor/testo405i_45823109/testo405i_45823109_temperature/config
homeassistant/sensor/testo405i_45823109/testo405i_45823109_velocity/config
```

Sensors automatically appear in Home Assistant as:
- **Testo 405i 45823109 Temperature** (°C)
- **Testo 405i 45823109 Velocity** (m/s)

---

## Averaging Behavior

The script accumulates all temperature and velocity samples received during the configured `sample_interval` and publishes the **average** of each metric. This reduces noise and provides more stable readings.

Example:
- `sample_interval: 5.0` → Publishes averaged values every 5 seconds
- If device sends 10 samples in 5 seconds, the published value is the average of those 10 samples

---

## CSV Logging

If enabled, measurements are appended to a CSV file:

```csv
timestamp,temperature,velocity
2025-11-09T09:15:40.951,21.073,1.635
2025-11-09T09:15:46.102,21.085,1.642
```

---

## Telegraf Integration

To ingest data into InfluxDB using Telegraf, use this configuration:

```toml
[[inputs.mqtt_consumer]]
  servers = ["tcp://localhost:1883"]
  topics = ["testo405i/+/state"]
  client_id = "telegraf_testo405i"
  qos = 1
  data_format = "json"
  name_override = "testo405i"

[[processors.regex]]
  namepass = ["testo405i"]
  [[processors.regex.tags]]
    key = "topic"
    pattern = "testo405i/([^/]+)/state"
    replacement = "${1}"

[[processors.converter]]
  namepass = ["testo405i"]
  [processors.converter.fields]
    float = ["Temperature", "Velocity"]
```

The `topic` tag will contain the device ID (e.g., `testo405i_45823109`).

---

## Troubleshooting

### BLE Connection Issues
```bash
# Check Bluetooth adapter
hciconfig

# Scan for devices
sudo hcitool lescan

# Check adapter permissions
sudo usermod -a -G bluetooth $USER
```

### MQTT Issues
```bash
# Test MQTT connection
mosquitto_sub -h homeassistant.local -t 'testo405i/#' -v

# Check if broker is reachable
ping homeassistant.local
```

### Service Logs
```bash
# View recent logs
sudo journalctl -u testo405i-mqtt -n 100

# Follow logs in real-time
sudo journalctl -u testo405i-mqtt -f

# Check for errors
sudo journalctl -u testo405i-mqtt -p err
```

### Common Issues

**"config.yaml not found"**
- Copy `config_example.yaml` to `config.yaml` and edit it

**"Testo 405i device not found"**
- Check device is powered on and in range
- Specify MAC address in config if auto-discovery fails
- Verify BLE adapter is working: `hciconfig`

**"MQTT server unreachable"**
- Verify broker address and port
- Check firewall rules
- Test with `mosquitto_sub`

**Permission denied (Bluetooth)**
- Add user to bluetooth group: `sudo usermod -a -G bluetooth <user>`
- Or run as root (not recommended for production)

---

## Requirements

| Component | Version |
|------------|----------|
| Python | ≥ 3.9 |
| Bleak | ≥ 0.22 |
| paho-mqtt | ≥ 1.6 |
| PyYAML | ≥ 6.0 |

---

## Project Structure

```
testo405i_mqtt/
├── testo405i_mqtt.py      # Main script
├── config.yaml            # Configuration (create from example)
├── config_example.yaml    # Configuration template
├── requirements.txt       # Python dependencies
├── README.md             # This file
├── LICENSE               # License information
└── testo405i.log         # Log file (created at runtime)
```

---

## License

See LICENSE file for details.

---

## Contributing

Contributions are welcome! Please feel free to submit issues or pull requests.

---

## Author

Developed for automation of Testo 405i BLE measurements and integration into MQTT-based monitoring systems, with focus on Home Assistant integration.
