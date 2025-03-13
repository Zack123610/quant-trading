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
    # Kafka Producer Configuration
    producer = KafkaProducer(bootstrap_servers='localhost:9092',
                             value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    async def fetch_and_send_bid_ask():
        url = 'https://www.investing.com/currencies/eur-usd-spreads'
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
        }
        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')
            bid_element = soup.find("span", class_="inlineblock pid-1-bid")
            ask_element = soup.find("span", class_="inlineblock pid-1-ask")

            if bid_element and ask_element:
                bid_value = float(bid_element.text.replace(',', ''))
                ask_value = float(ask_element.text.replace(',', ''))
                message = {"bid": bid_value, "ask": ask_value}

                # Send message to Kafka
                producer.send('eurusd_bidask', value=message)
                producer.flush()
                print("Message sent by Producer: ", message)
            else:
                print("Bid or Ask not found")
        else:
            print("Error fetching data: ", response.status_code)

    # Run the function every 5 seconds
    while not stop_flag:
        await fetch_and_send_bid_ask()
        await asyncio.sleep(5)
    
    producer.close()

def kafka_consumer_bot():
    consumer = KafkaConsumer('eurusd_bidask', 
                             bootstrap_servers='localhost:9092',
                             value_deserializer=lambda v: json.loads(v.decode('utf-8')),
                             auto_offset_reset='latest',
                             enable_auto_commit=False,
                             group_id='eurusd_bot'
                             )
    
    df = pd.DataFrame(columns=['Bid', 'Ask', 'Mid_Price'], dtype=float)

    print(f"Consumer is running... Listening to topic: {consumer.topics()}")
    
    for message in consumer:
        if stop_flag:
            break

        print(f"Message received by Consumer: {message.value}")

        bid = float(message.value['bid'])
        ask = float(message.value['ask'])
        mid_price = (bid + ask) / 2

        new_row = pd.DataFrame({'Bid': [bid], 'Ask': [ask], 'Mid_Price': [mid_price]})
        df = pd.concat([df, new_row], ignore_index=True)

        print(f"Consumer bid price: {bid:.4f}, ask price: {ask:.4f}, mid price: {mid_price:.4f}")
        print("Updated DataFrame: \n", df)

        # keep the last 10 rows
        if len(df) > 10:
            df = df.iloc[-10:].reset_index(drop=True)
        
        time.sleep(5)

async def main():
    # Start the Kafka Producer in event loop
    producer_task = asyncio.create_task(kafka_producer())

    # Start the Kafka Consumer in a separate thread
    consumer_thread = threading.Thread(target=kafka_consumer_bot, daemon=True)
    consumer_thread.start()

    try:
        await producer_task
    except asyncio.CancelledError:
        pass
    finally:
        consumer_thread.join()

try:
    nest_asyncio.apply()
    asyncio.run(main())
except KeyboardInterrupt:
    stop_flag = True
    print("Stopping the program...")
