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

# Global list to store messages
message_list = []

# MQTT Callbacks
def on_connect(client, userdata, flags, rc, properties=None):
    logging.info("Connected with result code %s", str(rc))
    client.subscribe("environment/database/#")

def on_message(client, userdata, msg):
    global message_list
    payload = msg.payload.decode()
    logging.info("Received message: %s", payload)
    try:
        message_json = json.loads(payload)
    except json.JSONDecodeError:
        message_json = {"error": "Invalid JSON"}
    
    message_list.append(message_json)
    logging.info("Updated message_list: %s", message_list)

# Initialize and configure the MQTT client
client = mqtt.Client(protocol=mqtt.MQTTv5)
client.on_connect = on_connect
client.on_message = on_message

client.tls_set()  # Setup TLS, adjust with proper certs if necessary
client.username_pw_set("hivemq.webclient.1721189577614", "UT801Op#W,N4$oayjDq.")  # Update with actual credentials
client.connect("olivealkali-pos5qd.a01.euc1.aws.hivemq.cloud", 8883, 60)  # Connect with TLS on port 8883

# Start MQTT loop in a separate thread
def mqtt_loop():
    client.loop_forever()

mqtt_thread = threading.Thread(target=mqtt_loop)
mqtt_thread.start()

# Flask route to retrieve the latest message
@app.route('/latest_message', methods=['GET'])
def get_latest_message():
    logging.info("Fetching latest message")
    if message_list:
        logging.info("Latest message: %s", message_list[-1])
        return jsonify({"latest_message": message_list[-1]})
    else:
        logging.info("No messages found")
        return jsonify({"latest_message": {}})

# Flask route to retrieve all messages
@app.route('/all_messages', methods=['GET'])
def get_all_messages():
    logging.info("Fetching all messages")
    return jsonify({"all_messages": message_list})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)




