from threading import Thread
from flask import Flask

from products.pulse import Pulse

app = Flask(__name__)
pulseIns = Pulse(verbose=True)


@app.route('/')
def home():
    # TODO - Get interest from the function
    return 'Hello'


@app.route('/pulse')
def pulse():
    thread = Thread(target=pulseIns.run)
    thread.daemon = True

    thread.start()
    return 'Pulse started'


@app.route('/stopPulse')
def stop_pulse():
    pulseIns.stop = True
    return 'Pulse stopped'


if __name__ == '__main__':
    app.run(threaded=True)
