import csv
import json
import time
from kafka import KafkaProducer
import os
import sys


KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'transactions')
CSV_FILE = os.getenv('CSV_FILE', '/data/train.csv')

def create_producer():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        acks='all',
        retries=3
    )
    return producer


def load_csv_to_kafka(csv_file, producer, topic):
    print(f"Starting to load data from {csv_file} to topic {topic}")
    sys.stdout.flush()
    count = 0
    with open(csv_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            row['amount'] = float(row['amount'])
            producer.send(topic, value=row)
            count += 1
            if count % 50000 == 0:
                print(f"Processed {count} rows...")
                sys.stdout.flush()
            if count >= 100000:
                break

        producer.flush()
    print(f"Loaded {count} rows to Kafka topic {topic}")
    sys.stdout.flush()


def main():
    print("=" * 50)
    print("Kafka Data Loader")
    print(f"Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Topic: {KAFKA_TOPIC}")
    print(f"CSV: {CSV_FILE}")
    print("=" * 50)
    sys.stdout.flush()
    
    producer = create_producer()
    load_csv_to_kafka(CSV_FILE, producer, KAFKA_TOPIC)
    producer.close()
    print("Done!")
    sys.stdout.flush()


if __name__ == "__main__":
    main()
