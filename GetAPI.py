from flask import Flask, jsonify
from flask_cors import CORS  
import threading
import paho.mqtt.client as mqtt
import json
import sqlite3

app = Flask(__name__)
CORS(app)

def init_db():
    conn = sqlite3.connect('carcount.db')
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

def on_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        client.subscribe("environment/LotLocate/#")
    else:
        print(f"Failed to connect")

def on_disconnect(client, userdata, rc):
    print("Disconnected")
    if rc != 0:
        print("Unexpected disconnection.")


def on_message(client, userdata, msg):
    payload = msg.payload.decode()
    try:
        message_json = json.loads(payload)
        print(f"Decoded JSON message: {message_json}")
    except json.JSONDecodeError:
        print("Failed to decode JSON")
        message_json = {"error": "Invalid JSON"}

    # Insert message into SQLite3 database
    try:
        conn = sqlite3.connect('carcount.db')
        c = conn.cursor()
        c.execute('INSERT INTO messages (topic, payload) VALUES (?, ?)', (msg.topic, json.dumps(message_json)))
        conn.commit()
        conn.close()
        print("Stored message in SQLite3 database")
    except sqlite3.Error as e:
        print(f"SQLite error: {e}")

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
            print("Starting MQTT loop")
            client.loop_forever()
        except Exception as e:
            print(f"MQTT loop error: {e}")
            client.reconnect()

mqtt_thread = threading.Thread(target=mqtt_loop)
mqtt_thread.start()
print("Started MQTT thread")

# Flask route to retrieve the latest message
@app.route('/latest_message', methods=['GET'])
def get_latest_message():
    print("Fetching latest message")
    try:
        conn = sqlite3.connect('carcount.db')
        c = conn.cursor()
        c.execute('SELECT payload FROM messages ORDER BY id DESC LIMIT 1')
        row = c.fetchone()
        conn.close()

        if row:
            try:
                latest_message = json.loads(row[0])  
                print(f"Latest message: {latest_message}")
                result = {"items": {
                    "IncomingCar": latest_message.get("IncomingCar"),
                    "OutgoingCar": latest_message.get("OutgoingCar"),
                    "TotalSlots": latest_message.get("TotalSlots"),
                    "Totalavailable": latest_message.get("Totalavailable")
                }}
                return jsonify(result)
            except json.JSONDecodeError:
                print("Failed to decode JSON from database")
                return jsonify({"error": "Invalid JSON in database"}), 500
        else:
            print("No messages found")
            return jsonify({})
    except sqlite3.Error as e:
        print(f"SQLite error: {e}")
        return jsonify({"error": "Database error"}), 500

if __name__ == '__main__':
    app.run() 



