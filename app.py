import os
from flask import Flask, jsonify

app = Flask(__name__)

@app.route("/")
def index():
    return "<h1>Flask is working!</h1><p>DATABASE_URL set: " + str(bool(os.environ.get("DATABASE_URL"))) + "</p>"

@app.route("/health")
def health():
    return jsonify({"status": "ok", "db_url_set": bool(os.environ.get("DATABASE_URL"))})

if __name__ == "__main__":
    app.run(port=int(os.environ.get("PORT", 5001)))
