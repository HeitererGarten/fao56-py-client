# py-iot-server

MQTT client to write data from mosquitto MQTT Broker.

---

## Installation

Clone this repo.

Create and activate a virtual environment.

Run this command to install all dependencies:

```Bash
pip install -r requirements.txt
```  

Create a `.env` file in the root dir as follow (Do not use quotations for strings):

```.env
MQTT_USERNAME=<broker-specified-username>
MQTT_PASSWORD=<broker-specified-pass>
```

Adjust `host-ip` in `config.yaml` to your broker's IP Address.

> [!note]
> To know your broker IP Address (on Windows), open `cmd` in Admin mode, run `ipconfig`, and check for your IPv4.

Only run `main.py` after running all other components of the system (Broker, Hub, Node).
