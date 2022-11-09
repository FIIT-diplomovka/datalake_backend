import os
import logging
from minio import Minio

class ObjectStorage:
    def __init__(self):
        self.mc = Minio(
            os.environ.get("MINIO_URL"),
            access_key=os.environ.get("MINIO_ACCESS_KEY"),
            secret_key=os.environ.get("MINIO_SECRET_KEY"),
            secure=False)

    def upload_new_file(self, file, filename, file_size=None):
        bucket = os.environ.get("STAGING_BUCKET")
        if file_size is not None:
            result = self.mc.put_object(bucket, filename, file, file_size, metadata={"stage": "new"})
        else:
            # object size is unknown
            result = self.mc.put_object(bucket, filename, file, -1, part_size=10*1024*1024, metadata={"stage": "new"})
        return bucket, result.object_name

    def check_metadata_extraction_stage(self, bucket, object_name):
        result = self.mc.stat_object(bucket, object_name)
        logging.info(result.metadata)
        return result
    
    def is_connected(self):
        try:
            if not self.mc.bucket_exists("fakedatalakebucket"):
                logging.info("Connected to object storage")
                return True
        except Exception as e:
            logging.critical("Cannot connect to object storage")
            logging.critical(e, exc_info=True)
            return False