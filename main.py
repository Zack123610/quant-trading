import requests
from bs4 import BeautifulSoup
import json
import numpy as np
from numpy import nan as npNan
import pandas as pd
import pandas_ta as ta
import asyncio
import threading
import nest_asyncio
import time
import os

from kafka import KafkaProducer, KafkaConsumer
from llama_index.core.program import LLMTextCompletionProgram
from llama_index.multi_modal_llms.openai import OpenAIMultiModal
from llama_index.core import SimpleDirectoryReader
from llama_index.core.workflow import Workflow, step, Event, Context
from llama_index.core.bridge.pydantic import BaseModel, Field
from llama_index.llms.openai import OpenAI
from typing import Optional, Any

nest_asyncio.apply()

# # Set your OpenAI API key to the environment variable
# os.environ['OPENAI_API_KEY'] = "YOUR_OPENAI_API_KEY"

# Global flag to stop the program
stop_flag = False

def stop_listener():
    global stop_flag
    while True:
        user_input = input("Enter 'stop' to stop the program:")
        if user_input.lower() == 'stop':
            stop_flag = True
            print("Stopping the program...")
            break

# Start a single thread for the stop listener
threading.Thread(target=stop_listener, daemon=True).start()


class TradingDecisionResult(BaseModel):
    """
    Model used to store the trading decision result.
    """
    decision: str = Field(description="Trading actions: 'buy', 'sell'or 'hold'")
    reasoning: str = Field(description="Reasoning behind the trading decision")


# Initialize workflow
class InvestmentBotWorkflow(Workflow):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.image_analysis_done = False

    async def analyze_image(self, ctx: Context, ev: Event) -> None:
        if not self.image_analysis_done:
            print("Analyzing the image...")
            openai_mm_llm = OpenAIMultiModal(
                model="gpt-4o",
                api_key=os.getenv("OPENAI_API_KEY"),
                max_new_tokens=512
            )
            image_path = "img/eurousd_chart.png"
            image_documents = SimpleDirectoryReader(input_files=[image_path]).load()
            response = openai_mm_llm.complete(
                prompt="Analyze the EUR/USD daily chart provided in the image and provide insights on the price trend.",
                documents=image_documents
            )
            response_text = response[0].text
            await ctx.set("image_analysis", response_text)
            self.image_analysis_done = True
            print("Image analysis completed.", response_text)

    @step
    async def analyze_data(self, ctx: Context, ev: Event) -> None:
        '''
        Executing data analysis step and provide trade decision based on technical indicators.
        '''
        df = await ctx.get('df')
        if df is None or df.empty:
            print("Dataframe is empty. No data to analyze.")
            return

        # Get image analysis result
        image_analysis = await ctx.get('image_analysis', "No analysis")
    
        if len(df) < 6:
            print("Not enough data to analyze.")
            return
        
        # Adjust indicator periods based on the data length
        ema_period = min(5, len(df))
        rsi_period = min(5, len(df))
        bb_period = min(5, len(df))

        # Calculate technical indicators
        df['EMA_5'] = ta.ema(df['Mid_Price'], length=ema_period)
        df['RSI'] = ta.rsi(df['Mid_Price'], length=rsi_period)
        bb = ta.bbands(df['Mid_Price'], length=bb_period, std=2)

        if bb is not None and not bb.empty:
            bb_columns = bb.columns.tolist()

            bbl_column = [col for col in bb_columns if col.startswith('BBL')]
            bbm_column = [col for col in bb_columns if col.startswith('BBM')]
            bbu_column = [col for col in bb_columns if col.startswith('BBU')]

            bb_selected = bb[[bbl_column[0], bbm_column[0], bbu_column[0]]]
            bb_selected.columns = ['BBL', 'BBM', 'BBU']

            df = pd.concat([df, bb_selected], axis=1)

        else:
            # Get the last 5 prices
            last_prices = df['Mid_Price'].tail(5).tolist()
        
        latest_data = df.iloc[-1]

        # Dealing with missing values
        indicators = {}
        indicators['EMA_5'] = latest_data.get('EMA_5', 'Not available')
        indicators['RSI'] = latest_data.get('RSI', 'Not available')
        indicators['BBL'] = latest_data.get('BBL', 'Not available')
        indicators['BBM'] = latest_data.get('BBM', 'Not available')
        indicators['BBU'] = latest_data.get('BBU', 'Not available')

        for key, value in indicators.items():
            if pd.isna(value):
                print(f"Missing value for {key}. Skipping the trade decision.")
                indicators[key] = 'Not available'
                
        # Prompts for GPT3
        prompt = (
            f"Latest price: {last_prices}\n"
            f"Latest technical indicators:\n"
            f"EMA 5: {indicators['EMA_5']}\n"
            f"RSI: {indicators['RSI']}\n"
            f"BBL: {indicators['BBL']} \n"
            f"BBM: {indicators['BBM']} \n"
            f"BBU: {indicators['BBU']} \n"
            f"EURUSD Daily Chart Analysis: {image_analysis}\n"
            "Based on the analysis, provide a trading decision: 'buy', 'sell' or 'hold'. Explain the reasoning behind the decision."
        )

        try:
            llm_gpt3 = OpenAI(
                model="gpt-3.5-turbo", 
                api_key=os.getenv("OPENAI_API_KEY"),
                max_new_tokens=512
            )
            program = LLMTextCompletionProgram.from_defaults(
                output_cls=TradingDecisionResult,
                prompt_template_str=prompt,
                llm=llm_gpt3
            )
            trading_decision_result = program()
            decision = trading_decision_result.decision
            reasoning = trading_decision_result.reasoning
            print(f"Trading Decision: {decision}")
            print(f"Reasoning: {reasoning}")
        except Exception as e:
            print(f"Error occurred while generating trading decision: {e}")


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
    # nest_asyncio.apply()
    asyncio.run(main())
except KeyboardInterrupt:
    stop_flag = True
    print("Stopping the program...")
