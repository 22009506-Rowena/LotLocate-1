from flask import Flask, jsonify
import requests

app = Flask(__name__)

@app.route('/latest_message', methods=['GET'])
def get_latest_message():
    print("Fetching latest message")
    try:
        # Make a GET request to the external API
        response = requests.get('https://meow-inyy.onrender.com/get_latest_record')
        
        # Check if the request was successful
        if response.status_code == 200:
            latest_message = response.json()
            print(f"Latest message: {latest_message}")
            return jsonify(latest_message)
        else:
            print(f"Failed to fetch latest message: {response.status_code}")
            return jsonify({"error": "Failed to fetch latest message"}), response.status_code

    except requests.RequestException as e:
        print(f"Request error: {e}")
        return jsonify({"error": "Request error"}), 500

if __name__ == '__main__':
    app.run(debug=True)

