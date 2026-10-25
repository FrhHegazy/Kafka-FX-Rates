from kafka import KafkaProducer
import requests
import time
import json
import os
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("EXCHANGE_RATE_API_KEY")
API_URL = f"https://v6.exchangerate-api.com/v6/{API_KEY}/latest/USD"
KAFKA_TOPIC = "currency-rates"

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print("Producer started. Fetching currency rates every 30 seconds...")

while True:
    try:
        response = requests.get(API_URL)
        data = response.json()
        
        if "conversion_rates" in data:
            print(f"Fetched {len(data['conversion_rates'])} currency rates")
            producer.send(KAFKA_TOPIC, data["conversion_rates"])
            producer.flush()
        else:
            print("Error: Unexpected API response")
    
    except Exception as e:
        print(f"Error: {e}")
    
    time.sleep(30)