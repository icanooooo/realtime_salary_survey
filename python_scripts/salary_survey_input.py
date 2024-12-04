from confluent_kafka import Producer
import json
import time
from datetime import datetime
from helper.kafka_helper import send_message

producer = Producer({"bootstrap.servers":"localhost:9092"}) # Input local machine port address

if __name__ == "__main__":
    send_message("salary_survey", producer)