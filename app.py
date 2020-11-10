from flask import Flask

app = Flask(__name__)


@app.route('/')
def home():
    # TODO - Get interest from the function
    return 'Hello'


if __name__ == '__main__':
    app.run(threaded=True)
