from flask import Blueprint, request
import base64
from utilities.object_storage_connector import ObjectStorage
from utilities.kafka_connector import Kafka
from utilities.postgres_connector import Postgres

write = Blueprint("write_routes", __name__, url_prefix="/write")

METADATA_PREFIX = "x-amz-meta-"



if not ObjectStorage().is_connected():
    exit(1)


@write.route("/upload", methods=["POST"])
def upload_file():
    kafka = Kafka()
    mc = ObjectStorage()
    f = request.files["user_file"]
    size = int(request.form["file_size"])
    bucket, object_name = mc.upload_new_file(f, f.filename, size)
    kafka.new_file_alert(bucket, object_name)
    # encode bucket + object name as b64 string. This way, front end can put it inside the URL parameters
    b64_address = (bucket + "/" + object_name).encode('ascii')
    b64_address = base64.b64encode(b64_address)
    b64_address = b64_address.decode('ascii')
    return {"bucket": bucket, "name": object_name, "b64": b64_address}, 201

@write.route("/submit_new", methods=["POST"])
def submit_new():
    mc = ObjectStorage()
    pg = Postgres()
    data = request.json
    new_bucket, new_object = mc.production_insert(data["object"]["bucket"], data["object"]["name"], data["dcm"])
    pg.insert_new_object(new_bucket, new_object, data["dcm"], data["tags"])
    return "Created", 201