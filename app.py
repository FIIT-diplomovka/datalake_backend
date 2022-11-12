from dotenv import load_dotenv
import os
if os.path.exists("./.env"):
    load_dotenv()
from flask import Flask, request
from flask_cors import CORS
import logging
from utilities.object_storage_connector import ObjectStorage
from utilities.kafka_connector import Kafka
from minio import Minio
import base64

METADATA_PREFIX = "x-amz-meta-"
app = Flask(__name__)
cors = CORS(app, resources={r"*": {"origins": "*"}})

kafka = Kafka()
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
    bucket, object_name = mc.upload_new_file(f, f.filename, size)
    kafka.new_file_alert(bucket, object_name)
    # encode bucket + object name as b64 string
    b64_address = (bucket + "/" + object_name).encode('ascii')
    b64_address = base64.b64encode(b64_address)
    b64_address = b64_address.decode('ascii')
    return {"bucket": bucket, "name": object_name, "b64": b64_address}, 201

@app.route("/check_stage", methods=["POST"])
def check_metadata_extraction_stage():
    data = request.json
    extracted = mc.check_metadata_extraction_stage(data["bucket"], data["name"])
    if (METADATA_PREFIX + "stage") in extracted and extracted[METADATA_PREFIX + "stage"] == "done":
        res = {}
        for key in extracted:
            if key.startswith(METADATA_PREFIX):
                value = extracted[key]
                key = key.replace(METADATA_PREFIX, "")
                res[key] = value
        res["etag"] = extracted["etag"].replace("\"", "")
        res["dcm_date"] = res["dcm_date"].split("T")[0]
        return res, 200
    else:
        return "Not ready yet..", 202


if __name__ == "__main__":
    app.run("0.0.0.0")