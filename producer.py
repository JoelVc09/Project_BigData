
import time
import random
import json
from kafka import KafkaProducer

# Configure your Kafka producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',  # Adjust if needed
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Function to generate simulated water quality data
def generate_water_quality_data():
    data = {
        'timestamp': time.time(),
        'pH': round(random.uniform(6.5, 9.5), 2),         # pH level
        'turbidity': round(random.uniform(0.1, 100.0), 2), # NTU (Nephelometric Turbidity Units)
        'dissolved_oxygen': round(random.uniform(0.0, 14.0), 2), # mg/L
        'temperature': round(random.uniform(0, 30), 2)    # °C
    }
    return data

# Main loop to send data to Kafka
if __name__ == "__main__":
    topic_name = 'water_quality_data'  # Name of your Kafka topic

    while True:
        water_quality_data = generate_water_quality_data()
        producer.send(topic_name, water_quality_data)
        print(f"Sent: {water_quality_data}")
        time.sleep(1)  # Adjust the frequency of data generation as needed
