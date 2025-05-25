import requests
from time import sleep
from datetime import datetime
import random
import json
import sqlalchemy
from sqlalchemy import text
import yaml
from fastapi import FastAPI, Query
from kafka import KafkaProducer, KafkaConsumer
import uvicorn
from threading import Thread
from uuid import UUID
import pandas as pd

random.seed(100)

class AWSDBConnector:
    """
    Handles database connection using credentials from a YAML file.
    """

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
            return {}
        
    def create_db_connector(self):
        """
        Creates a SQLAlchemy engine using credentials from the YAML file.

        Returns:
            sqlalchemy.engine.Engine: A SQLAlchemy database engine.
        """
        HOST = self.db_creds.get("HOST")
        USER = self.db_creds.get("USER")
        PASSWORD = self.db_creds.get("PASSWORD")
        DATABASE = self.db_creds.get("DATABASE")
        PORT = self.db_creds.get("PORT")
        return sqlalchemy.create_engine(f"postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")


def convert_non_serializables(obj):
    """
    Converts non-JSON-serializable values to serializable formats (UUID, datetime).

    Args:
        obj (dict): Dictionary potentially containing unserializable values.

    Returns:
        dict: A JSON-serializable dictionary.
    """
    serializable_obj = obj.copy()
    for key, value in serializable_obj.items():
        if isinstance(value, datetime):
            serializable_obj[key] = value.isoformat()
        elif isinstance(value, UUID):
            serializable_obj[key] = str(value)
    return serializable_obj


def run_infinite_post_data_loop(engine):
    """
    Continuously pulls random rows from three tables, posts to API endpoints,
    and saves batches to CSV after completion.
    
    Args:
        engine (sqlalchemy.engine.Engine): Database connection engine.
    """
    pin_data, geo_data, user_data = [], [], []
    count = 0

    with engine.connect() as connection:
        idx_query = text("SELECT idx FROM pinterest_data")
        all_idxs = [row[0] for row in connection.execute(idx_query).fetchall()]
    
    while count < 500:
        sleep(random.uniform(0, 1))
        random_idx = random.choice(all_idxs)
        
        with engine.connect() as connection:
            pin_row = connection.execute(text("SELECT * FROM pinterest_data WHERE idx = :idx"), {"idx": random_idx}).fetchone()
            geo_row = connection.execute(text("SELECT * FROM geolocation_data WHERE idx = :idx"), {"idx": random_idx}).fetchone()
            user_row = connection.execute(text("SELECT * FROM user_data WHERE idx = :idx"), {"idx": random_idx}).fetchone()
            
            if not all([pin_row, geo_row, user_row]):
                continue
            
            pin_result = dict(pin_row._mapping)
            geo_result = dict(geo_row._mapping)
            user_result = dict(user_row._mapping)

            pin_data.append(pin_result)
            geo_data.append(geo_result)
            user_data.append(user_result)

            print(pin_result)
            print(geo_result)
            print(user_result)

            headers = {'Content-Type': 'application/json'}

            geo_result = convert_non_serializables(geo_result)
            requests.post("http://localhost:8000/send_data?topic=pin_data.geo", json=geo_result, headers=headers)
            
            pin_result = convert_non_serializables(pin_result)
            requests.post("http://localhost:8000/send_data?topic=pin_data.pins", json=pin_result, headers=headers)
            
            user_result = convert_non_serializables(user_result)
            requests.post("http://localhost:8000/send_data?topic=pin_data.users", json=user_result, headers=headers)
            
            count += 1

    pd.DataFrame(pin_data).to_csv("/home/kyleoc/pinterest_data_pipeline/csv_files/pinterest_batch.csv", index=False)
    pd.DataFrame(geo_data).to_csv("/home/kyleoc/pinterest_data_pipeline/csv_files/geolocation_batch.csv", index=False)
    pd.DataFrame(user_data).to_csv("/home/kyleoc/pinterest_data_pipeline/csv_files/user_batch.csv", index=False)


api = FastAPI()

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    client_id="data_producer",
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

@api.get("/get_data")
def retrieve_data(topic: str = Query(...)):
    """
    Retrieve the most recent message from a given Kafka topic.

    Args:
        topic (str): Kafka topic to consume from.

    Returns:
        dict: Message value or a message stating the topic is empty.
    """
    temp_consumer = KafkaConsumer(
        topic,
        bootstrap_servers="localhost:9092",
        auto_offset_reset="earliest",
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        consumer_timeout_ms=1000
    )
    for msg in temp_consumer:
        return msg.value
    return {"message": f"No messages found on topic {topic}"}


@api.post("/send_data")
def send_data(payload: dict, topic: str):
    """
    Send data to a Kafka topic.

    Args:
        payload (dict): Data to send.
        topic (str): Kafka topic to publish to.

    Returns:
        dict: Confirmation message.
    """
    print(f"Sending to topic {topic}: {payload}")
    producer.send(topic, value=payload)
    producer.flush()
    return {"status": "Message sent"}


def run_webserver():
    """
    Starts the FastAPI web server using Uvicorn.
    """
    uvicorn.run(api, host="localhost", port=8000)


if __name__ == "__main__":
    new_connector = AWSDBConnector()
    Thread(target=run_webserver, daemon=True).start()
    run_infinite_post_data_loop(new_connector.engine)