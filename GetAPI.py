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
        
        all_messages = []
        for row in rows:
            try:
                message = json.loads(row[0])
                # Extract necessary details
                result ={"items": {"IncomingCar": message.get("IncomingCar"),"OutgoingCar": message.get("OutgoingCar"),"TotalSlots": message.get("TotalSlots"),"Totalavailable": message.get("Totalavailable")
                }}
                all_messages.append(result)
            except json.JSONDecodeError:
                logging.error("Failed to decode JSON from database row: %s", row)
                all_messages.append({"error": "Invalid JSON"})
        
        return jsonify(all_messages)
    except sqlite3.Error as e:
        logging.error("SQLite error: %s", e)
        return jsonify({"error": "Database error"}), 500

