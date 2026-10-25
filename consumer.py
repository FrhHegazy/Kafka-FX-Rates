from kafka import KafkaConsumer
import json
import os
from dotenv import load_dotenv

load_dotenv()

KAFKA_TOPIC = "currency-rates"
KAFKA_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_SERVER,
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='console-consumer'
)

print("Listening for currency rates updates...")
print("Press Ctrl+C to stop\n")

try:
    for message in consumer:
        rates = message.value
        print(f"\nReceived update (Message #{message.offset}):")
        print(f"Timestamp: {message.timestamp}")
        print(f"Total currencies: {len(rates)}")
        
        print("\nSample Rates:")
        for currency in ['USD', 'EUR', 'GBP', 'JPY', 'AED']:
            if currency in rates:
                print(f"{currency}: {rates[currency]:.4f}")
        
        print("-" * 50)

except KeyboardInterrupt:
    print("\nConsumer stopped by user")
finally:
    consumer.close()