from threading import Thread

from flask import Flask

from products.pulse import Pulse

app = Flask(__name__)

pulse = Pulse(verbose=True)


@app.route('/')
def home():
    # TODO - Get interest from the function
    return 'Hello'


@app.route('/pulse')
def pulse():
    thread = Thread(target=pulse.inc)
    thread.daemon=True

    thread.start()
    return 'Pulse started'


if __name__ == '__main__':
    app.run(threaded=True)
