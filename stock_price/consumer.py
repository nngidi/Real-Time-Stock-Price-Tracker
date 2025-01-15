from confluent_kafka import DeserializingConsumer
from confluent_kafka.serialization import StringDeserializer
import psycopg2
import json
from datetime import datetime

# Kafka consumer configuration
consumer_config = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'debug_group',
    'auto.offset.reset': 'earliest',
    'key.deserializer': StringDeserializer('utf_8'),
    'value.deserializer': StringDeserializer('utf_8'),
}

consumer = DeserializingConsumer(consumer_config)
consumer.subscribe(['stock_prices'])

print("Debugging consumer...")

# PostgreSQL configuration
db_config = {
    'dbname': 'stock_data',
    'user': 'postgres',
    'password': 'password',
    'host': 'localhost',
    'port': '5432'
}

# Connect to PostgreSQL
conn = psycopg2.connect(**db_config)
cursor = conn.cursor()

# Create table if it doesn't exist
cursor.execute("""
    CREATE TABLE IF NOT EXISTS prices (
        id SERIAL PRIMARY KEY,
        symbol TEXT,
        price NUMERIC,
        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
""")
conn.commit()

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue

        # Parse and print the received Kafka message
        message_value = msg.value()
        print(f"Message received: {message_value}")

        # Convert JSON string to a Python dictionary
        try:
            record = json.loads(message_value)
            symbol = record.get("symbol")
            price = record.get("price")
            unix_timestamp = record.get("timestamp")

            # Convert Unix timestamp to datetime
            if unix_timestamp:
                timestamp = datetime.fromtimestamp(unix_timestamp)
            else:
                timestamp = None

            # Insert the record into PostgreSQL
            cursor.execute("""
                INSERT INTO prices (symbol, price, timestamp)
                VALUES (%s, %s, %s)
            """, (symbol, price, timestamp))
            conn.commit()
            print(f"Inserted into PostgreSQL: {record}")
        except json.JSONDecodeError as e:
            print(f"Failed to decode JSON: {e}")
        except Exception as e:
            print(f"Error inserting into PostgreSQL: {e}")

except KeyboardInterrupt:
    print("Stopping consumer.")
finally:
    consumer.close()
    cursor.close()
    conn.close()
