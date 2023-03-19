from flask import Blueprint, request
from utilities.object_storage_connector import ObjectStorage
from utilities.postgres_connector import Postgres


read = Blueprint("read_routes", __name__, url_prefix="/read")

METADATA_PREFIX = "x-amz-meta-"


if not ObjectStorage().is_connected():
    exit(1)

# @read.route("/check_stage", methods=["POST"])
# def check_metadata_extraction_stage():
#     mc = ObjectStorage()
#     data = request.json
#     extracted = mc.check_metadata_extraction_stage(data["bucket"], data["name"])
#     if (METADATA_PREFIX + "stage") in extracted and extracted[METADATA_PREFIX + "stage"] == "done":
#         res = {}
#         for key in extracted:
#             if key.startswith(METADATA_PREFIX):
#                 value = extracted[key]
#                 key = key.replace(METADATA_PREFIX, "")
#                 res[key] = value
#         res["etag"] = extracted["etag"].replace("\"", "")
#         res["dcm_date"] = res["dcm_date"].split("T")[0]
#         return res, 200
#     else:
#         return "Not ready yet..", 202
    
@read.route("/check_stage", methods=["POST"])
def check_metadata_extraction_stage():
    data = request.json
    pg = Postgres()
    check = pg.check_staging(data["bucket"], data["name"])
    if check == None:
        return "Not ready yet...", 202
    if check["metadata"] is not None:
        check["metadata"]["dcm_date"] = check["metadata"]["dcm_date"].split("T")[0]
    return check, 200

@read.route("/check_malware", methods=["POST"])
def check_malware_check_phase():
    data = request.json
    pg = Postgres()
    check = pg.check_staging(data["bucket"], data["name"])
    if check is None:
        return "Not ready yet...", 202
    else:
        return {"malware": check["malware"]}
         



@read.route("/get_objects", methods=["GET"])
def get_objects():
    args = request.args
    tags = args.get("tags", default=None, type=str)
    pg = Postgres()
    objects = pg.get_objects(tags=tags)
    results = {
        "total": len(objects),
        "objects": objects
    }
    return results, 200

@read.route("/details/<id>", methods=["GET"])
def get_details(id):
    pg = Postgres()
    result = pg.get_details(id)
    if result is None:
        return "Object does not exist", 404
    return result, 200

@read.route("/get_tags", methods=["GET"])
def get_tags():
    pg = Postgres()
    result = {
        "tags": pg.get_tags()
    }
    return result, 200