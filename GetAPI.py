from flask import Flask, request, jsonify

app = Flask(__name__)

# In-memory database
car_data = []

@app.route('/add_record', methods=['POST'])
def add_record():
    if not request.json or not 'total_slots' in request.json:
        return jsonify({"error": "Invalid input"}), 400
    
    record = {
        'total_slots': request.json['total_slots'],
        'total_available': request.json['total_available'],
        'incoming_car': request.json['incoming_car'],
        'outgoing_car': request.json['outgoing_car']
    }
    car_data.append(record)
    return jsonify({"message": "Record added successfully"}), 201

@app.route('/latest_message', methods=['GET'])
def get_latest_record():
    if car_data:
        latest_record = car_data[-1]
        response = {
            "items": {
                "IncomingCar": latest_record['incoming_car'],
                "OutgoingCar": latest_record['outgoing_car'],
                "TotalSlots": latest_record['total_slots'],
                "TotalAvailable": latest_record['total_available']
            }
        }
        return jsonify(response), 200
    else:
        return jsonify({"error": "No records found"}), 404

if __name__ == '__main__':
    app.run(debug=True)


