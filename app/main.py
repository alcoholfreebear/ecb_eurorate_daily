# import concurrent.futures
from flask import Flask, request
from operations import seed_historical, append_new

app = Flask(__name__)

@app.route("/eurorate/etl")
def app_etl():
   return append_new()

@app.route("/eurorate/seed")
def app_seed():
    return seed_historical()


if __name__ == "__main__":
    # This is used when running locally only. When deploying to Google App
    # Engine, a webserver process such as Gunicorn will serve the app. This
    # can be configured by adding an `entrypoint` to app.yaml.
    RUN_WS = False
    if RUN_WS:
        app.run(host="127.0.0.1", port=8080, debug=True)
    else:
        append_new()
