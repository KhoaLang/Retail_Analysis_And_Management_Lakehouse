import json
import time
import pandas as pd
from kafka import KafkaProducer
from datetime import datetime, UTC

KAFKA_BROKER = "localhost:9092"
TOPIC_NAME = "retail_orders_raw"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

FILE_PATH = "./ingestion/source/retail_data.csv"
df = pd.read_csv(FILE_PATH)

df = df[:1]

print(f"Loaded {len(df)} records from source file")

for index, row in df.iterrows():
    event = {
        "source_system": "retail_oltp",
        "table_name": "orders",
        "operation": "INSERT",
        "event_time": datetime.now(UTC).isoformat(),
        "data": row.to_dict()
    }

    producer.send(TOPIC_NAME, value=event)

    time.sleep(0.1)

producer.flush()
producer.close()

print("Finished sending data to Kafka")
