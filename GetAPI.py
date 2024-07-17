from flask import Flask, jsonify
from flask_cors import CORS  
import threading
import paho.mqtt.client as mqtt
import os
import logging
import sqlite3

# Setup logging
logging.basicConfig(level=logging.DEBUG)

app = Flask(__name__)
CORS(app)

# Initialize SQLite3 database
def init_db():
    conn = sqlite3.connect('mqtt_messages.db')
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            topic TEXT,
            payload INTEGER
        )
    ''')
    conn.commit()
    conn.close()

init_db()

# MQTT Callbacks
def on_connect(client, userdata, flags, rc, properties=None):
    logging.info("Connected with result code %s", str(rc))
    client.subscribe("environment/database/#")

def on_message(client, userdata, msg):
    payload = msg.payload.decode()
    logging.info("Received message: %s", payload)
    try:
        payload_int = int(payload)
        logging.info("Converted payload to integer: %d", payload_int)
    except ValueError:
        logging.error("Failed to convert payload to integer")
        payload_int = None
    
    if payload_int is not None:
        # Insert message into SQLite3 database
        try:
            conn = sqlite3.connect('mqtt_messages.db')
            c = conn.cursor()
            c.execute('INSERT INTO messages (topic, payload) VALUES (?, ?)', (msg.topic, payload_int))
            conn.commit()
            conn.close()
            logging.info("Stored message in SQLite3 database")
        except sqlite3.Error as e:
            logging.error("SQLite error: %s", e)

# Initialize and configure the MQTT client
client = mqtt.Client(protocol=mqtt.MQTTv5)
client.on_connect = on_connect
client.on_message = on_message

client.tls_set()  # Setup TLS, adjust with proper certs if necessary
client.username_pw_set("hivemq.webclient.1721189577614", "UT801Op#W,N4$oayjDq.")  # Update with actual credentials
client.connect("olivealkali-pos5qd.a01.euc1.aws.hivemq.cloud", 8883, 60)  # Connect with TLS on port 8883

# Start MQTT loop in a separate thread
def mqtt_loop():
    logging.info("Starting MQTT loop")
    client.loop_forever()

mqtt_thread = threading.Thread(target=mqtt_loop)
mqtt_thread.start()
logging.info("Started MQTT thread")

# Flask route to retrieve the latest message
@app.route('/latest_message', methods=['GET'])
def get_latest_message():
    logging.info("Fetching latest message")
    try:
        conn = sqlite3.connect('mqtt_messages.db')
        c = conn.cursor()
        c.execute('SELECT payload FROM messages ORDER BY id DESC LIMIT 1')
        row = c.fetchone()
        conn.close()
        
        if row:
            latest_message = row[0]
            logging.info("Latest message: %d", latest_message)
            return jsonify({"latest_message": latest_message})
        else:
            logging.info("No messages found")
            return jsonify({"latest_message": None})
    except sqlite3.Error as e:
        logging.error("SQLite error: %s", e)
        return jsonify({"error": "Database error"}), 500

# Flask route to retrieve all messages
@app.route('/all_messages', methods=['GET'])
def get_all_messages():
    logging.info("Fetching all messages")
    try:
        conn = sqlite3.connect('mqtt_messages.db')
        c = conn.cursor()
        c.execute('SELECT payload FROM messages')
        rows = c.fetchall()
        conn.close()
        
        all_messages = [row[0] for row in rows]
        return jsonify({"all_messages": all_messages})
    except sqlite3.Error as e:
        logging.error("SQLite error: %s", e)
        return jsonify({"error": "Database error"}), 500

if __name__ == '__main__':
    app.run()



