from kafka import KafkaProducer
import os
import json

class Kafka:
    def __init__(self):
        self.producer = KafkaProducer(bootstrap_servers=os.environ.get("KAFKA_URL"))
    

    def start_metadata_analysis(self, bucket, path, method="droid"):
        msg_body = {
            "bucket": bucket,
            "path": path,
            "method": method
        }
        msg_body = json.dumps(msg_body)
        self.producer.send("NEW_ENTRY", str.encode(msg_body))
