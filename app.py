import sys
from pathlib import Path

from modules.DataManager import DataManager
from products.pulse import Pulse

sys.path.append(str(Path(__file__).resolve().parent.parent))
from threading import Thread
from flask import Flask, jsonify
from flask_cors import CORS, cross_origin
from config import stock_list

app = Flask(__name__)

cors = CORS(app)
app.config['CORS_HEADERS'] = 'Content-Type'

threads_dict = {}
# webInterface = WebInterface(start_world=True)
pulse = Pulse(verbose=True)


@app.route('/')
def home():
    # TODO - Get interest from the function
    return 'Hello'


@app.route('/interest')
def get_interest():
    # TODO - Get interest from the function
    return 'Script is running'


@app.route('/stocks')
def get_stocks_list():
    return jsonify(stock_list)


# @app.route('/terminate')
# def terminate_script():
#     webInterface.stop = True
#     return 'terminated'


# @app.route('/monitor')
# def monitor_script():
#     thread = Thread(target=webInterface.run)
#     thread.daemon = True
#
#     if not thread.is_alive():
#         thread.start()
#         threads_dict['monitor'] = thread
#     return 'monitor'


# @cross_origin()
# @app.route('/getFile')
# def read_from_file():
#     # return jsonify(webInterface.read_from_json())
#     return jsonify(webInterface.read_from_db())


@app.route('/pulse')
def pulse_script():
    thread = Thread(target=pulse.run)
    thread.daemon = True

    if not thread.is_alive():
        thread.start()
        threads_dict['pulse'] = thread
    return 'monitor'


@cross_origin()
@app.route('/getPulse')
def read_from_db():
    dataManager = DataManager()
    return jsonify(dataManager.read_from_db())


if __name__ == '__main__':
    app.run(threaded=True)
