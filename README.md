# data-producer

A Python script that replays network router metrics from a CSV file into a Kafka topic.

## What it does

Reads a CSV of historical router metrics, sorted by timestamp, and produces each row as a JSON message to Kafka — preserving the original timing between events at a configurable speed multiplier.

## Requirements

- Python 3
- `pandas`
- `confluent-kafka`

```bash
pip install pandas confluent-kafka
```

## Usage

```bash
python python_producer.py
```

## Configuration

All settings are controlled via environment variables:

| Variable          | Default                          | Description                          |
|-------------------|----------------------------------|--------------------------------------|
| `KAFKA_BOOTSTRAP` | `34.76.89.164:30994`             | Kafka broker address                 |
| `KAFKA_TOPIC`     | `router.metrics.raw`             | Topic to produce messages to         |
| `CSV_PATH`        | `router_metrics_1day_200devices.csv` | Path to the input CSV file       |
| `REPLAY_SPEED`    | `50.0`                           | Replay speed multiplier (1.0 = real-time) |

### Example: replay at real-time speed

```bash
REPLAY_SPEED=1.0 python python_producer.py
```

## Message format

Each message is a JSON object derived from a CSV row, with the `timestamp` field converted to ISO 8601 format. Messages are keyed by `device_id`.
