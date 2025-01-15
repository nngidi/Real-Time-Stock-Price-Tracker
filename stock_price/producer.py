import logging
import json
import time
import requests
from confluent_kafka import Producer
import signal
import sys

logging.basicConfig(level=logging.DEBUG)
API_URL = "https://finnhub.io/api/v1/quote"
API_KEY = "cu0e0lhr01ql96gqdaagcu0e0lhr01ql96gqdab0"  # Ensure this is correct
STOCKS = ["AAPL", "GOOGL", "MSFT"]

# Set up the Kafka producer
producer = Producer({'bootstrap.servers': 'localhost:9092'})

def signal_handler(sig, frame):
    print("Exiting the program.")
    producer.flush()  # Ensure Kafka messages are flushed before exiting
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

while True:
    for stock in STOCKS:
        response = requests.get(f"{API_URL}?symbol={stock}&token={API_KEY}")
        print(response.status_code)  # Check if the status is 200 OK
        print(response.text)  # Print the raw response body

        try:
            data = response.json()
            if 'c' in data:
                message = {
                    "symbol": stock,
                    "price": data['c'],  # Current price
                    "timestamp": data['t']
                }
                producer.produce('stock_prices', key=stock, value=json.dumps(message), callback=delivery_report)
            else:
                print(f"Price data for {stock} not found")
        except ValueError:
            print(f"Failed to parse JSON for {stock}")
            continue

    time.sleep(5)
