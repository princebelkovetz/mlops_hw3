from kafka import KafkaProducer
import json
import sys
import os
import csv 

KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
TRANSACTIONS_TOPIC = os.getenv('TRANSACTIONS_TOPIC', 'transactions')
CSV_FILE = os.getenv('CSV_FILE', '/data/train.csv')

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
)

def load_kafka():
    print(f"Loading data: file {CSV_FILE} -> topic {TRANSACTIONS_TOPIC}")
    sys.stdout.flush()
    with open(CSV_FILE, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        processed_rows = 0
        for row in reader:
            producer.send(TRANSACTIONS_TOPIC, value=row)
            processed_rows += 1
            if processed_rows % 100000 == 0:
                print(f"{processed_rows=}")
                sys.stdout.flush()

        producer.flush()
    print(f"Loading finished. {processed_rows=}")
    sys.stdout.flush()


def main():
    sys.stdout.flush()
    load_kafka()
    producer.close()
    sys.stdout.flush()


if __name__ == "__main__":
    main()
