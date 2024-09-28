import scrap
from flask import Flask, render_template, jsonify, send_file, render_template_string ,request
from datetime import datetime
from flask_cors import CORS
import json
import time


app = Flask(__name__)
CORS(app)

scap_status = 0


def calltime():
    now = datetime.today()
    return(now.ctime())

@app.route('/')
def home():
    return render_template('index.html')

@app.route('/movies', methods=['GET'])
def get_movies():
    try:
        with open('Top250andBoxoffice.json', 'r', encoding='utf-8') as json_file:
            movies_data = json.load(json_file)
        return jsonify(movies_data)
    except FileNotFoundError:
        return jsonify({"error": "File not found"}), 404
    except json.JSONDecodeError:
        return jsonify({"error": "Invalid JSON format"}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/scaping', methods=['GET'])
def get_scaping():
    global scap_status
    if scap_status == 0 :
        scap_status = 1
        try:
            print("start scraping",calltime())
            scrap.scrap()
            scap_status = 0
            print("scraping Succes",calltime())
            return jsonify({"message": "Scraping completed successfully."}), 200
        except Exception as e:
            return jsonify({"error": str(e)}), 500
    else:
        return jsonify({"message": "Gonna scaping"}), 200

@app.route('/status', methods=['GET'])
def get_status():
    global scap_status
    print(scap_status)
    return f"{scap_status}"

@app.route('/download')
def download_file():
    file_path = 'Top250andBoxoffice.json'
    return send_file(file_path, as_attachment=True)


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=10000)
