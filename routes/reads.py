from flask import Blueprint, request
from utilities.object_storage_connector import ObjectStorage
from utilities.postgres_connector import Postgres


read = Blueprint("read_routes", __name__, url_prefix="/read")

METADATA_PREFIX = "x-amz-meta-"


if not ObjectStorage().is_connected():
    exit(1)

@read.route("/check_stage", methods=["POST"])
def check_metadata_extraction_stage():
    mc = ObjectStorage()
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


@read.route("/get_objects", methods=["GET"])
def get_objects():
    pg = Postgres()
    return pg.get_objects(), 200

@read.route("/details/<id>", methods=["GET"])
def get_details(id):
    pg = Postgres()
    result = pg.get_details(id)
    if result is None:
        return "Object does not exist", 404
    return result, 200