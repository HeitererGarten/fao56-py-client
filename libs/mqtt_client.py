# MQTT Client using asyncio-mqtt
from dotenv import load_dotenv
from pathlib import Path 
import aiomqtt
import csv 
import flatdict
# orjson is significantly faster than std json lib
import orjson 
import os
import yaml 
import logging 


class MQTTHandler:
    def __init__(self):
        # Load sensitive credentials from .env
        if load_dotenv():
            self.mqtt_username = os.getenv("MQTT_USERNAME")
            self.mqtt_password = os.getenv("MQTT_PASSWORD")
        else:
            msg: str = "Missing .env file"
            logging.error(msg)
            raise FileNotFoundError(msg)
        
        # Get parent directory of /libs
        # First .parent gets /libs 
        # Second .parent gets PC_MQTT_Client/
        self.project_root = Path(__file__).parent.parent 
        
        # Load configuration settings from config.yaml 
        self.config: dict = self._load_config()
        self.host_ip: str = self.config["mqtt"]["host_ip"]
        self.port: int = self.config["mqtt"]["port"]
        self.topic_sensor: str = self.config["mqtt"]["topics"]["topic_sensor"]
        self.identifier: str = self.config["mqtt"]["identifier"]
        self.fieldnames: list = self.config["mqtt"]["fieldnames"]

    # Fetch data from PC_MQTT_Client/config.yaml or .yml
    def _load_config(self) -> dict:
        config_path_yaml: Path = self.project_root/f"config.yaml"
        config_path_yml: Path = self.project_root/f"config.yml"
        
        if config_path_yaml.exists():
            try:
                with open(config_path_yaml, "r", encoding="utf-8") as cfg_file:
                    return yaml.safe_load(cfg_file)
            except yaml.YAMLError as e:
                logging.error(f"Error parsing {config_path_yaml}: {e}")
        elif config_path_yml.exists():
            try:
                with open(config_path_yml, "r", encoding="utf-8") as cfg_file:
                    return yaml.safe_load(cfg_file)
            except yaml.YAMLError as e:
                logging.error(f"Error parsing {config_path_yml}: {e}")
        else:
            msg: str = f"Config file not found"
            logging.error(msg)
            raise FileNotFoundError(msg)
    
    # Turn bytestrings into single-level Python dictionaries to write to /db
    def _process_message(self, payload: bytes) -> None:
        # Convert bytestring into Python dictionary
        try:
            json_dict: dict = orjson.loads(payload)
            # Flatten nested JSON into a single level 
            flattened_dict: flatdict.FlatterDict = flatdict.FlatterDict(json_dict)
            # Fix key names (e.g. date:year=2025 -> year=2025)
            # New dict used as flatdict is susceptible to errors if modified while iterating over itself
            processed_dict: dict = dict()
            key_list: list = flattened_dict.keys()
            for key in key_list:
                # Assuming default delimiter (":") is used
                new_key: str = key.split(":")[-1]
                processed_dict[new_key] = flattened_dict[key]
                # TODO: Add a robust message filter private method to sort out unwanted values
            self._write_to_csv(processed_dict)
                
        except orjson.JSONDecodeError as e:
            logging.error(f"Error decoding JSON entry: {e}") 
            logging.debug(f"Data type received: {type(payload)}")
            
    # Write to /db/sensor_data.csv
    def _write_to_csv(self, processed_dict) -> None:
        db_path: Path = self.project_root/f"db"/f"sensor_data.csv"
        
        # Make new db directory if not existed, otherwise leave it untouched
        db_path.parent.mkdir(exist_ok=True)
        
        # Create new file with headers if not existed already
        if not db_path.exists():
            with open(db_path, "w", encoding="utf-8", newline='') as file:
                writer = csv.DictWriter(file, fieldnames=self.fieldnames)
                writer.writeheader()
        
        # Append dict entry
        try: 
            with open(db_path, "a", encoding="utf-8", newline="") as file:
                writer: csv.DictWriter = csv.DictWriter(file, fieldnames=self.fieldnames)
                writer.writerow(processed_dict)
        except IOError as e:
            logging.error(f"I/O Error occured: {e}")
        except OSError as e:
            logging.error(f"OS Error occured: {e}")
        except PermissionError as e:
            logging.error(f"Insufficient permission: {e}")
        except Exception as e:
            logging.error(f"Unexpected error occured while writing to db: {e}")
        
    # Main method. Ideally only run this after creating a MQTTHandler instance
    async def run(self) -> None:
        # Init client  
        async with aiomqtt.Client(
            identifier=self.identifier,
            hostname=self.host_ip, 
            port=self.port, 
            username=self.mqtt_username,
            password=self.mqtt_password,
            # Persistent connection 
            clean_session=False
        ) as client:
            await client.subscribe(self.topic_sensor)
            async for message in client.messages:
                logging.info(f"Received: {message.payload}")
                self._process_message(message.payload)
                
