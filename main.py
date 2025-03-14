import json
import requests
import time
import pandas as pd
from kafka import KafkaProducer, KafkaConsumer
from bs4 import BeautifulSoup
import asyncio
import nest_asyncio
import threading

# Use threading.Event for better stop signal handling
stop_event = threading.Event()

async def kafka_producer():
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    async def fetch_and_send_bid_ask():
        url = 'https://www.investing.com/currencies/eur-usd-spreads'
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
        }
        try:
            response = requests.get(url, headers=headers, timeout=5)
            response.raise_for_status()

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

        except requests.RequestException as e:
            print(f"Error fetching data: {e}")

    try:
        while not stop_event.is_set():
            await fetch_and_send_bid_ask()
            await asyncio.sleep(5)
    except asyncio.CancelledError:
        pass
    finally:
        producer.close()
        print("Kafka Producer stopped.")

def kafka_consumer_bot():
    consumer = KafkaConsumer(
        'eurusd_bidask', 
        bootstrap_servers='localhost:9092',
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        auto_offset_reset='latest',
        enable_auto_commit=False,
        group_id='eurusd_bot'
    )
    
    # Initialize DataFrame without dtype
    df = pd.DataFrame(columns=['Bid', 'Ask', 'Mid_Price'])

    print(f"Consumer is running... Listening to topic: {consumer.topics()}")
    
    try:
        for message in consumer:
            if stop_event.is_set():
                break

            print(f"Message received by Consumer: {message.value}")

            bid = float(message.value['bid'])
            ask = float(message.value['ask'])
            mid_price = (bid + ask) / 2

            new_row = pd.DataFrame({'Bid': [bid], 'Ask': [ask], 'Mid_Price': [mid_price]})
            df = pd.concat([df, new_row], ignore_index=True)

            print(f"Consumer bid price: {bid:.4f}, ask price: {ask:.4f}, mid price: {mid_price:.4f}")
            print("Updated DataFrame: \n", df)

            # Keep the last 10 rows only
            if len(df) > 10:
                df = df.iloc[-10:].reset_index(drop=True)

            time.sleep(5)
    except Exception as e:
        print(f"Error in Consumer: {e}")
    finally:
        consumer.close()
        print("Kafka Consumer stopped.")

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
        stop_event.set()
        consumer_thread.join()
        print("Program stopped.")

if __name__ == "__main__":
    try:
        nest_asyncio.apply()
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Stopping the program...")
        stop_event.set()
