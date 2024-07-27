from flask import Flask, jsonify, Response
from flask_cors import CORS  
import threading
import paho.mqtt.client as mqtt
import json
import logging
import sqlite3
import time

# Setup logging
logging.basicConfig(level=logging.DEBUG)

app = Flask(__name__)
CORS(app)

# Initialize database
def init_db():
    conn = sqlite3.connect('carcount.db', check_same_thread=False)
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            topic TEXT,
            payload TEXT
        )
    ''')
    conn.commit()
    conn.close()

init_db()

# Database interaction function
def execute_db_query(query, params=()):
    conn = sqlite3.connect('carcount.db', check_same_thread=False)
    c = conn.cursor()
    c.execute(query, params)
    conn.commit()
    results = c.fetchall()
    conn.close()
    return results

# MQTT Callbacks
def on_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        logging.info("Connected to MQTT broker")
        client.subscribe("environment/LotLocate/#")
    else:
        logging.error("Failed to connect, return code %d\n", rc)

def on_disconnect(client, userdata, rc):
    logging.warning("Disconnected from MQTT broker")
    if rc != 0:
        logging.error("Unexpected disconnection.")
    client.reconnect()  # Reconnect if disconnected

def on_message(client, userdata, msg):
    payload = msg.payload.decode()
    logging.info("Received message: %s", payload)
    try:
        message_json = json.loads(payload)
        logging.info("Decoded JSON message: %s", message_json)
    except json.JSONDecodeError:
        logging.error("Failed to decode JSON")
        message_json = {"error": "Invalid JSON"}

    # Insert message into SQLite3 database
    try:
        execute_db_query('INSERT INTO messages (topic, payload) VALUES (?, ?)', (msg.topic, json.dumps(message_json)))
        logging.info("Stored message in SQLite3 database")
    except sqlite3.Error as e:
        logging.error("SQLite error: %s", e)

# Initialize and configure the MQTT client
client = mqtt.Client(protocol=mqtt.MQTTv5)
client.on_connect = on_connect
client.on_disconnect = on_disconnect
client.on_message = on_message

client.tls_set()  # Setup TLS, adjust with proper certs if necessary
client.username_pw_set("hivemq.webclient.1722080464728", "X<C@GS3yn4>k5a:gEKb7")
client.connect("olivealkali-pos5qd.a01.euc1.aws.hivemq.cloud", 8883, 60)

# Start MQTT loop in a separate thread
def mqtt_loop():
    while True:
        try:
            logging.info("Starting MQTT loop")
            client.loop_forever()
        except Exception as e:
            logging.error("MQTT loop error: %s", e)
            client.reconnect()

mqtt_thread = threading.Thread(target=mqtt_loop)
mqtt_thread.start()
logging.info("Started MQTT thread")

# Function to generate server-sent events
def generate_latest_message():
    last_id = 0
    while True:
        row = execute_db_query('SELECT id, payload FROM messages WHERE id > ? ORDER BY id DESC LIMIT 1', (last_id,))
        if row:
            last_id = row[0][0]
            try:
                latest_message = json.loads(row[0][1])  # Ensure the payload is returned as JSON
                logging.info("Latest message: %s", latest_message)
                result = {
                    "items": {
                        "IncomingCar": latest_message.get("IncomingCar"),
                        "OutgoingCar": latest_message.get("OutgoingCar"),
                        "TotalSlots": latest_message.get("TotalSlots"),
                        "Totalavailable": latest_message.get("Totalavailable")
                    }
                }
                yield f"data: {json.dumps(result)}\n\n"
            except json.JSONDecodeError:
                logging.error("Failed to decode JSON from database")
        time.sleep(1)

# Flask route to serve server-sent events
@app.route('/latest_message', methods=['GET'])
def get_latest_message():
    return Response(generate_latest_message(), content_type='text/event-stream')

# Flask route to retrieve all messages
@app.route('/all_messages', methods=['GET'])
def get_all_messages():
    logging.info("Fetching all messages")
    try:
        rows = execute_db_query('SELECT payload FROM messages')
        
        all_messages = []
        for row in rows:
            try:
                message = json.loads(row[0])
                result = {"items": {
                    "IncomingCar": message.get("IncomingCar"),
                    "OutgoingCar": message.get("OutgoingCar"),
                    "TotalSlots": message.get("TotalSlots"),
                    "Totalavailable": message.get("Totalavailable")
                }}
                all_messages.append(result)
            except json.JSONDecodeError:
                logging.error("Failed to decode JSON from database row: %s", row)
                all_messages.append({"error": "Invalid JSON"})
        
        return jsonify(all_messages)
    except sqlite3.Error as e:
        logging.error("SQLite error: %s", e)
        return jsonify({"error": "Database error"}), 500

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')  # Set host to '0.0.0.0' for external access
