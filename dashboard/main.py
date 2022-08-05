from flask import Flask

app = Flask(__name__, static_folder='./', static_url_path='')


@app.route('/')
def index():
    return app.send_static_file('./out/index.html')


if __name__ == '__main__':
    app.run(threaded=True, port=5000)