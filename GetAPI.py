from flask import Flask, jsonify
from flask_cors import CORS  
import threading
import paho.mqtt.client as mqtt
import json
import os
import logging

# Setup logging
logging.basicConfig(level=logging.DEBUG)

app = Flask(__name__)
CORS(app)

# Global variable to store the latest message
latest_message = {}

# MQTT Callbacks
def on_connect(client, userdata, flags, rc, properties=None):
    logging.info("Connected with result code %s", str(rc))
    client.subscribe("environment/database/#")

def on_message(client, userdata, msg):
    global latest_message
    payload = msg.payload.decode()
    logging.info("Received message: %s", payload)
    try:
        message_json = json.loads(payload)
    except json.JSONDecodeError:
        message_json = {"error": "Invalid JSON"}
    
    latest_message = {
        "items": message_json
    }
    logging.info("Updated latest_message: %s", latest_message)

# Initialize and configure the MQTT client
client = mqtt.Client(protocol=mqtt.MQTTv5)
client.on_connect = on_connect
client.on_message = on_message

client.tls_set()  # Setup TLS, adjust with proper certs if necessary
client.username_pw_set("hivemq.webclient.1720262723667", "#WC%8?ib9&BDpa36qAEf")  # Update with actual credentials
client.connect("05b6d7489be942a8843f0528e4c87a7e.s1.eu.hivemq.cloud", 8883, 60)  # Connect with TLS on port 8883

# Start MQTT loop in a separate thread
def mqtt_loop():
    client.loop_forever()

mqtt_thread = threading.Thread(target=mqtt_loop)
mqtt_thread.start()

# Flask route to retrieve the latest message
@app.route('/latest_message', methods=['GET'])
def get_latest_message():
    return jsonify(latest_message)

if __name__ == '__main__':
    app.run()


