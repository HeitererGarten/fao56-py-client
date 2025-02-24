import logging 
import asyncio
import sys, os
from libs.mqtt_client import MQTTHandler

def main(): 
    # Setup logging functionalities (Used by libraries in /libs)
    logging.basicConfig(level=logging.DEBUG)
    
    # Edge case on Windows where its default event loop is insufficient for aiomqtt
    if sys.platform.lower() == "win32" or os.name.lower() == "nt":
        from asyncio import set_event_loop_policy, WindowsSelectorEventLoopPolicy
        set_event_loop_policy(WindowsSelectorEventLoopPolicy())
    
    # Init an instance with all the settings 
    mqtt_handler = MQTTHandler()
    
    # Init the event loop
    asyncio.run(mqtt_handler.run())
    

if __name__ == "__main__": 
    main()
