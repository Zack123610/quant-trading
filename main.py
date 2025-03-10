import json
import requests
import time
import pandas as pd
from kafka import KafkaProducer, KafkaConsumer
from bs4 import BeautifulSoup
import asyncio
import nest_asyncio
import threading

stop_flag = False

async def kafka_producer():
    producer = KafkaProducer(bootstrap_servers='localhost:9092',
                             value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    while not stop_flag:
        try:
            response = requests.get("https://www.tradingview.com/symbols/EURUSD/")
            soup = BeautifulSoup(response.text, 'html.parser')
            price = soup.find("div", {"class": "tv-symbol-price-quote__value js-symbol-last"}).text
            producer.send('eurusd', {"price": price})
        except Exception as e:
            print(e)
        time.sleep(5)

# Kafka Producer Configuration
producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))
