from dotenv import load_dotenv
import os
if os.path.exists("./.env"):
    load_dotenv()
from flask import Flask, request
from flask_cors import CORS
import logging
from utilities.object_storage_connector import ObjectStorage
from minio import Minio

app = Flask(__name__)
cors = CORS(app, resources={r"*": {"origins": "*"}})

mc = ObjectStorage()

if not mc.is_connected():
    exit(1)

@app.route("/")
def index():
    return "<h1>datalake<h1>"

@app.route("/upload", methods=["POST"])
def upload_file():
    f = request.files["user_file"]
    size = int(request.form["file_size"])
    mc.upload_new_file(f, f.filename, size)
    return "", 200


if __name__ == "__main__":
    app.run("0.0.0.0")