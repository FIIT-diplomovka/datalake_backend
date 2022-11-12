from flask import Blueprint, request
import base64
from utilities.object_storage_connector import ObjectStorage
from utilities.kafka_connector import Kafka
from minio import Minio

read = Blueprint("read_routes", __name__, url_prefix="/read")

METADATA_PREFIX = "x-amz-meta-"

kafka = Kafka()
mc = ObjectStorage()

if not mc.is_connected():
    exit(1)

@read.route("/check_stage", methods=["POST"])
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