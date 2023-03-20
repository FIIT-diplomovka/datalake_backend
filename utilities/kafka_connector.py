from kafka import KafkaProducer
import os
import json

class Kafka:
    def __init__(self):
        self.producer = KafkaProducer(bootstrap_servers=os.environ.get("KAFKA_URL"))
    

    def start_metadata_analysis(self, bucket, path, method="droid", use_gpt3=False):
        msg_body = {
            "bucket": bucket,
            "path": path,
            "method": method,
            "use_gpt3": use_gpt3
        }
        msg_body = json.dumps(msg_body)
        self.producer.send("NEW_ENTRY", str.encode(msg_body))
