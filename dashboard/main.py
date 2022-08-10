from flask import Flask
import os

app = Flask(__name__, static_folder='./out/', static_url_path='')


@app.route('/')
def index():
    return app.send_static_file('index.html')

@app.route('/<type>')
def type(type):
    return app.send_static_file('[type].html')

@app.route('/<type>/<entity>')
def entity(type, entity):
    return app.send_static_file('[type]/[entity].html')

@app.route('/static/<asset>')
def deliver_static(asset):
    return app.send_static_file('static/' + asset)

if __name__ == '__main__':
    app.run(threaded=True, port=os.getenv("DASHBOARD_PORT", 5000))