import paho.mqtt.client as mqtt

import json

import time

import datetime

import random



# Replace the following with your AWS MQ broker details

mqtt_broker = "b-66af2f31-2e02-42c1-8827-6fc3fde602f8-1.mq.ap-south-1.amazonaws.com"

mqtt_port = 8883

mqtt_username = "mqtt"

mqtt_password = "ADC53F72D32Et"



# Create an MQTT client instance

client = mqtt.Client()



# Set up the TLS/SSL security options (required for AWS MQ)

client.tls_set()



# Set the username and password for authentication

client.username_pw_set(username=mqtt_username, password=mqtt_password)



# Connect to the MQTT broker

connection_timeout = 1200

client.connect(mqtt_broker, mqtt_port, keepalive=connection_timeout)



# Start the MQTT loop to handle incoming and outgoing messages

client.loop_start()



# Function to generate dummy sensor data (replace this with actual data)

def generate_dummy_sensor_data():

    current_time = datetime.datetime.now().strftime("%H:%M:%S")

    mileage=100

    mil_chan=random.randint(0,3)

    mileage=mileage-mil_chan

    return {

        "uuid": "12345678-1212-2323-3434-454524046422",

        "time": current_time,

        "speed": random.randint(0, 120),

        "mileage": mileage,

        "voltage": round(random.uniform(65, 82), 2),

        "latitude": round(random.uniform(12.777645, 12.85325), 6),

        "longitude": round(random.uniform(77.562614, 77.80620), 6)

    }



# Your main program logic goes here

try:

    while True:

        # Generate dummy sensor data with proper current time

        sensor_data = generate_dummy_sensor_data()



        # Convert sensor data to JSON format

        payload = json.dumps(sensor_data)



        # Publish the JSON payload to the topic "vehicle_data"

        client.publish("vehicle_data", payload)



        print("Published:", payload)

        time.sleep(20)  # Publish data every 20 seconds

except KeyboardInterrupt:

    pass



# Stop the MQTT loop and disconnect

client.loop_stop()

client.disconnect()

