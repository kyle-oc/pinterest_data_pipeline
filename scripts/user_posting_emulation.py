import requests
from time import sleep
from datetime import datetime
import random
import json
import sqlalchemy
from sqlalchemy import text
import yaml
from fastapi import FastAPI, Request, Query
from kafka import KafkaProducer
from kafka import KafkaConsumer
import uvicorn
from threading import Thread
from uuid import UUID

random.seed(100)

class AWSDBConnector:

    def __init__(self):
        self.yaml_file = "/home/kyleoc/pinterest_data_pipeline/local_db_creds.yaml"
        self.db_creds = self.read_db_creds()        
        self.engine = self.create_db_connector()
        
    def read_db_creds(self):
        """
        Reads database credentials from the YAML file.

        Returns:
            dict: A dictionary containing database connection credentials.
        """
        try:
            with open(self.yaml_file, 'r') as y:
                return yaml.safe_load(y)
        except FileNotFoundError:
            print(f"\nError: The file {self.yaml_file} was not found.\n")
            return {}
        except yaml.YAMLError as e:
            print(f"Error: Problem parsing the YAML file - {e}")
            return {} # Return empty dict if error occurs 
        
    def create_db_connector(self):
        HOST = self.db_creds.get("HOST")
        USER = self.db_creds.get("USER")
        PASSWORD = self.db_creds.get("PASSWORD")
        DATABASE = self.db_creds.get("DATABASE")
        PORT = self.db_creds.get("PORT")
        engine = sqlalchemy.create_engine(f"postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
        return engine


new_connector = AWSDBConnector()

def run_infinite_post_data_loop(engine):
    while True:
        sleep(random.randrange(0, 2))
        random_row = random.randint(0, 11000)

        with engine.connect() as connection:

            pin_string = text(f"SELECT * FROM pinterest_data LIMIT 1 OFFSET {random_row}")
            pin_selected_row = connection.execute(pin_string)
            
            for row in pin_selected_row:
                pin_result = dict(row._mapping)

            geo_string = text(f"SELECT * FROM geolocation_data LIMIT 1 OFFSET {random_row}")
            geo_selected_row = connection.execute(geo_string)
            
            for row in geo_selected_row:
                geo_result = dict(row._mapping)

            user_string = text(f"SELECT * FROM user_data LIMIT 1 OFFSET {random_row}")
            user_selected_row = connection.execute(user_string)
            
            for row in user_selected_row:
                user_result = dict(row._mapping)
            
            print(pin_result)
            print(geo_result)
            print(user_result)

            headers = {'Content-Type': 'application/json'}

            # Milestone 4 Task 1
            geo_result = convert_non_serializables(geo_result)
            response_geo = requests.post("http://localhost:8000/send_data?topic=pin_data.geo", json=geo_result, headers=headers)
            
            pin_result = convert_non_serializables(pin_result)
            response_pins = requests.post("http://localhost:8000/send_data?topic=pin_data.pins", json=pin_result, headers=headers)
            
            user_result = convert_non_serializables(user_result)            
            response_users = requests.post("http://localhost:8000/send_data?topic=pin_data.users", json=user_result, headers=headers)
            
    
def convert_non_serializables(obj):
    serializable_obj = obj.copy()
    for key, value in serializable_obj.items():
        if isinstance(value, datetime):
            serializable_obj[key] = value.isoformat()
        elif isinstance(value, UUID):
            serializable_obj[key] = str(value)
    return serializable_obj


if __name__ == "__main__":
    # Milestone 4 Task 2
    api = FastAPI()


    # Milestone 4 Task 1
    producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    client_id="data_producer",
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

    # # Milestone 4 task 2   
    @api.get("/get_data")
    def retrieve_data(topic: str = Query(...)):
        temp_consumer = KafkaConsumer(
            topic,
            bootstrap_servers="localhost:9092",
            auto_offset_reset="earliest",
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            consumer_timeout_ms=1000  # Stop after 1 second if no messages
        )
        for msg in temp_consumer:
            return msg.value
        return {"message": f"No messages found on topic {topic}"}

    # Milestone 4 task 2
    @api.post("/send_data")
    def send_data(payload: dict, topic: str):
        print(f"Sending to topic {topic}: {payload}")
        producer.send(topic, value=payload)
        producer.flush()
        return {"status": "Message sent"}

    # Milestone 4 task 2
    def run_webserver():
        uvicorn.run(api, host="localhost", port=8000)

    # Milestone 4 task 2
    Thread(target=run_webserver, daemon=True).start()
    run_infinite_post_data_loop(new_connector.engine)