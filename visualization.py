import matplotlib.pyplot as plt
from kafka import KafkaConsumer
import json
from collections import defaultdict
import matplotlib.animation as animation

# Kafka configuration
kafka_bootstrap_servers = 'localhost:9092'  # Adjust as necessary
input_topic = 'avg-ph-by-sensor-5-min'

# Create a Kafka consumer
consumer = KafkaConsumer(
    input_topic,
    bootstrap_servers=kafka_bootstrap_servers,
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=True
)

# Data structure to hold pH values
sensor_data = defaultdict(list)

# Function to update the plot
def update(frame):
    plt.clf()  # Clear the current figure
    for sensor_id, pH_values in sensor_data.items():
        plt.plot(pH_values, label=sensor_id)
    plt.title("Real-Time Average pH by Sensor")
    plt.xlabel("Time (Data Points)")
    plt.ylabel("Average pH")
    plt.legend()
    plt.ylim(6.0, 8.5)  # Adjust based on expected pH range
    plt.grid()

# Animation function
def animate():
    for message in consumer:
        data = message.value
        sensor_id = data['sensor_id']
        avg_pH = data['avg_pH']
        
        # Append new average pH value for the sensor
        sensor_data[sensor_id].append(avg_pH)
        
        # Limit the number of data points to display
        for key in sensor_data.keys():
            if len(sensor_data[key]) > 50:  # Keep the last 50 data points
                sensor_data[key].pop(0)

# Set up the plot
fig = plt.figure()
ani = animation.FuncAnimation(fig, update, interval=1000)

# Start the animation
try:
    animate()
    plt.show()
except KeyboardInterrupt:
    consumer.close()
