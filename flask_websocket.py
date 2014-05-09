__author__ = 'kevin'
import sys
from flask import Flask
from flask_uwsgi_websocket._gevent import GeventWebSocket

app = Flask(__name__)
ws = GeventWebSocket(app)

@ws.route('/echo')
def echo(ws):
    while True:
        msg = ws.receive()
        ws.send(msg)

if __name__ == '__main__':
    app.config["virtualenv"]='/home/kevin/workspace/python/fpserver/venv'
    app.config["pyhome"]='/home/kevin/workspace/python/fpserver/venv'
    app.config["venv"]='/home/kevin/workspace/python/fpserver/venv'
    app.config["home"]='/home/kevin/workspace/python/fpserver/venv'
    app.run(debug=True, host='localhost', port=8080, master=True, processes=8)
