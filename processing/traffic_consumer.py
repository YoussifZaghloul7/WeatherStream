# etl for traffic - reads raw traffic off kafka, works out a congestion
# percentage, writes it to postgres. mirrors the weather consumer

import json
import logging
import os
import sys
import time

import psycopg2
from kafka import KafkaConsumer
from kafka.errors import KafkaError

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common.config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC_TRAFFIC, postgres_dsn

logging.basicConfig(level=logging.INFO, format="%(asctime)s [traffic-consumer] %(message)s")
log = logging.getLogger(__name__)


def connect_consumer(retries: int = 10, delay: float = 5.0) -> KafkaConsumer:
    for attempt in range(1, retries + 1):
        try:
            return KafkaConsumer(
                KAFKA_TOPIC_TRAFFIC,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                group_id="weatherstream-traffic-processing",
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                key_deserializer=lambda k: k.decode("utf-8") if k else None,
            )
        except KafkaError:
            log.warning("Kafka not reachable yet (attempt %d/%d), retrying in %.0fs...", attempt, retries, delay)
            time.sleep(delay)
    raise RuntimeError("Could not connect to Kafka after several retries")


def connect_postgres(retries: int = 10, delay: float = 5.0):
    for attempt in range(1, retries + 1):
        try:
            conn = psycopg2.connect(postgres_dsn())
            conn.autocommit = True
            return conn
        except psycopg2.OperationalError:
            log.warning("Postgres not reachable yet (attempt %d/%d), retrying in %.0fs...", attempt, retries, delay)
            time.sleep(delay)
    raise RuntimeError("Could not connect to Postgres after several retries")


def transform(record: dict) -> dict:
    current = record.get("current_speed")
    free_flow = record.get("free_flow_speed")

    congestion_pct = None
    if current is not None and free_flow:
        congestion_pct = round((1 - current / free_flow) * 100, 1)

    return {
        "location": record["location"],
        "current_speed_kmh": current,
        "free_flow_speed_kmh": free_flow,
        "congestion_pct": congestion_pct,
        "road_closure": bool(record.get("road_closure", False)),
    }


def store(conn, t: dict):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO traffic_processed (
                location, current_speed_kmh, free_flow_speed_kmh, congestion_pct, road_closure
            )
            VALUES (%s, %s, %s, %s, %s)
            """,
            (
                t["location"],
                t["current_speed_kmh"],
                t["free_flow_speed_kmh"],
                t["congestion_pct"],
                t["road_closure"],
            ),
        )


def run():
    consumer = connect_consumer()
    conn = connect_postgres()
    log.info("Connected to Kafka and Postgres. Waiting for messages on '%s'...", KAFKA_TOPIC_TRAFFIC)

    for message in consumer:
        record = message.value
        try:
            t = transform(record)
            store(conn, t)
            log.info(
                "Stored traffic for %s: %s km/h vs %s free-flow (%s%% congestion)",
                t["location"],
                t["current_speed_kmh"],
                t["free_flow_speed_kmh"],
                t["congestion_pct"],
            )
        except Exception:
            log.exception("Failed to process traffic record: %s", record)


if __name__ == "__main__":
    run()
