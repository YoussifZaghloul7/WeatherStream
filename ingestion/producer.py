# ingestion side of the pipeline - just polls open-meteo and dumps
# readings into kafka. nothing fancy, this is the data ingestion tool part


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
from common.config import CITIES, KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC_RAW, POLL_INTERVAL_SECONDS

logging.basicConfig(level=logging.INFO, format="%(asctime)s [producer] %(message)s")
log = logging.getLogger(__name__)

OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"
CURRENT_FIELDS = (
    "temperature_2m,relative_humidity_2m,apparent_temperature,"
    "precipitation,weather_code,wind_speed_10m,wind_direction_10m"
)


def fetch_weather(city: str, latitude: float, longitude: float) -> dict:
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "current": CURRENT_FIELDS,
        "timezone": "auto",
    }
    response = requests.get(OPEN_METEO_URL, params=params, timeout=15)
    response.raise_for_status()
    payload = response.json()

    return {
        "city": city,
        "latitude": latitude,
        "longitude": longitude,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "current": payload.get("current", {}),
        "current_units": payload.get("current_units", {}),
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
    producer = connect_producer()
    log.info("Connected to Kafka at %s, publishing to topic '%s'", KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC_RAW)
    log.info("Tracking %d cities, polling every %ds", len(CITIES), POLL_INTERVAL_SECONDS)

    while True:
        for city, lat, lon in CITIES:
            try:
                record = fetch_weather(city, lat, lon)
                producer.send(KAFKA_TOPIC_RAW, key=city, value=record)
                log.info("Published reading for %s: %s", city, record["current"])
            except requests.RequestException as exc:
                log.error("Failed to fetch weather for %s: %s", city, exc)
        producer.flush()
        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    run()
