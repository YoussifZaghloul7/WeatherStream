# pulls live traffic flow from tomtom and sends it to kafka. same idea as
# the weather producer, just a different api and topic

import json
import logging
import os
import sys
import time
from datetime import datetime, timezone

import requests
from kafka import KafkaProducer
from kafka.errors import KafkaError

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC_TRAFFIC,
    TOMTOM_API_KEY,
    TRAFFIC_LOCATIONS,
    TRAFFIC_POLL_INTERVAL_SECONDS,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [traffic-producer] %(message)s")
log = logging.getLogger(__name__)

TOMTOM_URL = "https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute/10/json"


def fetch_traffic(location: str, latitude: float, longitude: float) -> dict:
    params = {
        "point": f"{latitude},{longitude}",
        "key": TOMTOM_API_KEY,
    }
    response = requests.get(TOMTOM_URL, params=params, timeout=15)
    response.raise_for_status()
    payload = response.json().get("flowSegmentData", {})

    return {
        "location": location,
        "latitude": latitude,
        "longitude": longitude,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "current_speed": payload.get("currentSpeed"),
        "free_flow_speed": payload.get("freeFlowSpeed"),
        "road_closure": payload.get("roadClosure", False),
    }


def connect_producer(retries: int = 10, delay: float = 5.0) -> KafkaProducer:
    for attempt in range(1, retries + 1):
        try:
            return KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
            )
        except KafkaError:
            log.warning("Kafka not reachable yet (attempt %d/%d), retrying in %.0fs...", attempt, retries, delay)
            time.sleep(delay)
    raise RuntimeError("Could not connect to Kafka after several retries")


def run():
    if not TOMTOM_API_KEY:
        raise RuntimeError(
            "TOMTOM_API_KEY is not set. Get a free key at developer.tomtom.com "
            "and add it to your .env file."
        )

    producer = connect_producer()
    log.info("Connected to Kafka at %s, publishing to topic '%s'", KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC_TRAFFIC)
    log.info("Tracking %d location(s), polling every %ds", len(TRAFFIC_LOCATIONS), TRAFFIC_POLL_INTERVAL_SECONDS)

    while True:
        for location, lat, lon in TRAFFIC_LOCATIONS:
            try:
                record = fetch_traffic(location, lat, lon)
                producer.send(KAFKA_TOPIC_TRAFFIC, key=location, value=record)
                log.info("Published traffic for %s: %s", location, record)
            except requests.RequestException as exc:
                log.error("Failed to fetch traffic for %s: %s", location, exc)
        producer.flush()
        time.sleep(TRAFFIC_POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    run()
