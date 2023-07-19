import paho.mqtt.client as mqtt
import json
import pandas as pd
import time

# Replace the following with your AWS MQ broker details
mqtt_broker = "b-66af2f31-2e02-42c1-8827-6fc3fde602f8-1.mq.ap-south-1.amazonaws.com"
mqtt_port = 8883
mqtt_username = "mqtt"
mqtt_password = "ADC53F72D32Et"

# Create an empty DataFrame to store the data
df = pd.DataFrame(columns=["uuid", "time", "speed", "mileage", "voltage", "latitude", "longitude"])

# Callback when the client connects to the MQTT broker
def on_connect(client, userdata, flags, rc):
    print("Connected with result code " + str(rc))
    # Subscribe to the topic "vehicle_data"
    client.subscribe("vehicle_data")

# Callback when a message is received from the MQTT broker
def on_message(client, userdata, msg):
    try:
        # Decode the JSON payload and append it to the DataFrame
        payload = json.loads(msg.payload.decode('utf-8'))
        df.loc[len(df)] = payload

    except json.JSONDecodeError as e:
        print("Error decoding JSON:", e)

# Create an MQTT client instance
client = mqtt.Client()

# Set up the TLS/SSL security options (required for AWS MQ)
client.tls_set()

# Set the username and password for authentication
client.username_pw_set(username=mqtt_username, password=mqtt_password)

# Set the callback functions
client.on_connect = on_connect
client.on_message = on_message

# Connect to the MQTT broker
connection_timeout = 1200
client.connect(mqtt_broker, mqtt_port, keepalive=connection_timeout)

# Start the MQTT loop to handle incoming and outgoing messages
client.loop_start()

# Track the number of rows printed
rows_printed = 0
print("\nTrify")
# Main program logic
try:
    while True:
        # Wait for 20 seconds
        time.sleep(20)

        # Check if there are new rows to print
        if not df.empty and len(df) > rows_printed:
            # Print the received DataFrame as a table using pandas
            
            print(df.iloc[rows_printed:])  # Print only new rows
            rows_printed = len(df)  # Update the number of rows printed

except KeyboardInterrupt:
    pass

# Stop the MQTT loop and disconnect
client.loop_stop()
client.disconnect()
