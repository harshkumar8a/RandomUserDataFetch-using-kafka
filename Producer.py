from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
import logging
import uuid
import json
import time
import random   
import threading
import requests
from dotenv import load_dotenv
import os

load_dotenv()
API_KEY = os.getenv("API_KEY")


KAFKA_BROKER = "localhost:29092,localhost:39092,localhost:49092"
NUM_PARTITIONS = 5
REPLICATION_FACTOR = 3

logging.basicConfig(
    level= logging.INFO
)

logger = logging.getLogger(__name__)


TOPIC_NAME = "financial_transaction"
API_KEY_Topic_Name = "real_api_data"


producer_config = {
    "bootstrap.servers": KAFKA_BROKER,
    "queue.buffering.max.messages": 1000,
    "queue.buffering.max.kbytes": 512000,
    "batch.num.messages": 10,
    "linger.ms":10,
    "acks": 1,
    "compression.type": "gzip"
}

producer = Producer(producer_config)

def create_topic(topic_name):
    adminClient = AdminClient({
        "bootstrap.servers": KAFKA_BROKER
    })

    try:
        metadata = adminClient.list_topics(timeout = 10)
        if topic_name not in metadata.topics:
            newTopic = NewTopic(
                topic = topic_name,
                num_partitions=  NUM_PARTITIONS,
                replication_factor = REPLICATION_FACTOR
            )
            fs = adminClient.create_topics([newTopic])
            for topic, future in fs.items():
                try:
                    future.result()
                    logger.info(f"Topic {topic_name} created successfully")
                    
                except Exception as e:
                    logger.error(f"Failed to create topic {topic}: {e}")
        else:
            logger.info(f"Topic {topic_name} already exists")
    except Exception as e:
        logger.error(f"Error creating Topic: {e}")
     

def deliver_report(err, msg):
    if err is not None:
        logger.error(f"Message delivery failed: {msg.key}")
    else:
        logger.info(f"Record {msg.key()} successfully produced to topic {msg.topic()}")


def producer_transaction(thread_id):
    while True:
        transaction = generate_transaction()

        try:
            producer.produce(
                topic= TOPIC_NAME,
                key = transaction["userId"],
                value = json.dumps(transaction).encode("utf-8"),
                on_delivery= deliver_report
            )
            print(f"Thread {thread_id} - Produced transcation : {transaction}")
            producer.flush()
        except Exception as e:
            print(f"Error sending transaction: {e}")

def producer_api(thread_id):
    while True:
        try:
            api_url = 'https://api.api-ninjas.com/v2/randomuser'
            response = requests.get(api_url, headers={'X-Api-Key': API_KEY},timeout=5)
            if response.status_code == 200:
                data = response.json()
                # Convert dictionary to JSON string and send to topic 'real_api_data'
                producer.produce(API_KEY_Topic_Name, value=json.dumps(data), callback=deliver_report)
                print(f"Thread {thread_id} - Produced transcation : {data}")
                producer.flush()
            else:
                print(f"API Error: {response.status_code}")
        except requests.exceptions.Timeout:
            print("Request timed out")

        except requests.exceptions.ConnectionError:
            print("Connection failed")

        except Exception as e:
            print(f"Unexpected error: {e}")
        



def producer_data_in_parallel(num_thread):
    threads = []
    try:
        for i in range(num_thread):
            thread = threading.Thread(target=producer_api, args=(i,))
            thread.daemon = True  
            thread.start()
            threads.append(thread)

        for thread in threads:
            thread.join()
    
    except Exception as e:
        print(f"Error creating thread: {e} ")




if __name__ == "__main__":
    create_topic(API_KEY_Topic_Name)
    producer_data_in_parallel(5)


    

    
        
