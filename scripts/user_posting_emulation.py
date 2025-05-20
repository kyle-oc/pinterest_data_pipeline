import requests
from time import sleep
import random
from multiprocessing import Process
import boto3
import json
import sqlalchemy
from sqlalchemy import text
import psycopg2
import yaml
from fastapi import FastAPI, Request
from kafka import KafkaProducer
from kafka import KafkaConsumer
import uvicorn
from threading import Thread

random.seed(100)

class AWSDBConnector:

    def __init__(self):
        self.yaml_file = "/home/kyleoc/pinterest_data_pipeline/local_db_creds.yaml"
        self.db_creds = self.read_db_creds()        
        engine = self.create_db_connector()
        
    def read_db_creds(self):
        """
        Reads database credentials from the YAML file.

        Returns:
            dict: A dictionary containing database connection credentials.
        """
        try:
            with open(self.yaml_file, 'r') as y:
                print(y)
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


def run_infinite_post_data_loop():
    while True:
        sleep(random.randrange(0, 2))
        random_row = random.randint(0, 11000)
        engine = new_connector.create_db_connector()

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

            # TODO Milestone 4 Task 1
            # Note - This will work with the POST function
            # FastAPI will infer what the payload and topic are automatically!
            # Naturally this is only 1 topic you will have to replicate this for the other 2.
            # response_geo = requests.post("http://localhost:8000/send_data?topic=pin_data.geo",  data=json.dumps(geo_result, default=str))


if __name__ == "__main__":
    # TODO When you arrive at specific Milestones/Tasks uncomment the code associated!


    # TODO Milestone 4 Task 2
    # api = FastAPI()


    # TODO Milestone 4 Task 1
    # producer = KafkaProducer(
    #     bootstrap_servers="localhost:9092",
        # TODO uncomment and fill in below fields:
        #client_id= , # Name the producer apprioprately
        #value_serializer= # Hint - Serialise Python dict to bytes using JSON and encode using utf-8
    # )


    # TODO Milestone 4 task 1
    # consumer = KafkaConsumer(
    #     "pin_data.geo",
    #     bootstrap_servers="localhost:9092",
        # TODO uncomment and fill in below fields:
        #auto_offset_reset= ,
        #value_deserializer= # Hint - Load the JSON
    # )


    # TODO Milestone 4 task 2
    # @api.get("/get_data")
    # def retrieve_data():
    #     msg = next(consumer)
    #     return msg


    # TODO Milestone 4 task 2
    # @api.post("/send_data")
    # def send_data(payload: dict, topic: str):
    #     print(payload)
        # TODO send the data using the Kafka Producer
        # 
        # producer.flush() # This line ensures all messages are sent to Kafka broker
        

    # TODO Milestone 4 task 2
    # def run_webserver():
    #     uvicorn.run(app=api, host="localhost", port=8000)
    #     return 
    

    # TODO Milestone 4 task 2
    # Thread(target=run_webserver, daemon=True).start()


    run_infinite_post_data_loop()
    print('Working')
    
    


