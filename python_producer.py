import json
import os
import time
import pandas as pd
from confluent_kafka import Producer

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "34.76.89.164:30994")
TOPIC = os.getenv("KAFKA_TOPIC", "router.metrics.raw")
CSV_PATH = os.getenv("CSV_PATH", "router_metrics_1day_200devices.csv")

# Replay speed factor:
# 1.0 = real time
# 10.0 = 10x faster
# 100.0 = 100x faster
REPLAY_SPEED = float(os.getenv("REPLAY_SPEED", "50.0"))
# ==================

def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")

def main():
    print(f"Loading CSV from {CSV_PATH}")
    df = pd.read_csv(CSV_PATH, parse_dates=["timestamp"])
    df = df.sort_values("timestamp")

    p = Producer({
        "bootstrap.servers": BOOTSTRAP,
        "socket.timeout.ms": 10000,
        "message.timeout.ms": 30000,
    })

    print(f"Replaying to {TOPIC} via {BOOTSTRAP} at {REPLAY_SPEED}x speed")

    previous_ts = None

    for _, row in df.iterrows():
        event = row.to_dict()

        # Convert timestamp to ISO string
        event["timestamp"] = event["timestamp"].isoformat()

        # Control replay speed
        current_ts = pd.to_datetime(event["timestamp"])
        if previous_ts is not None:
            delta = (current_ts - previous_ts).total_seconds()
            sleep_time = delta / REPLAY_SPEED
            if sleep_time > 0:
                time.sleep(sleep_time)

        previous_ts = current_ts

        key = str(event["device_id"]).encode("utf-8")
        value = json.dumps(event).encode("utf-8")

        p.produce(TOPIC, key=key, value=value, callback=delivery_report)
        p.poll(0)

    print("Finished replay.")
    p.flush()

if __name__ == "__main__":
    main()