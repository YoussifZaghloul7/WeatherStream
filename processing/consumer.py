# the etl part - reads raw weather off kafka, cleans it up, flags extreme
# weather, writes both raw + cleaned versions into postgres


import json
import logging
import os
import sys
import time
from datetime import datetime

import psycopg2
from kafka import KafkaConsumer
from kafka.errors import KafkaError

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common.config import (
    EXTREME_PRECIPITATION_MM,
    EXTREME_TEMP_HIGH_C,
    EXTREME_TEMP_LOW_C,
    EXTREME_WIND_KMH,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC_RAW,
    postgres_dsn,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [consumer] %(message)s")
log = logging.getLogger(__name__)

WMO_WEATHER_CODES = {
    0: "Clear sky",
    1: "Mainly clear",
    2: "Partly cloudy",
    3: "Overcast",
    45: "Fog",
    48: "Depositing rime fog",
    51: "Light drizzle",
    53: "Moderate drizzle",
    55: "Dense drizzle",
    56: "Light freezing drizzle",
    57: "Dense freezing drizzle",
    61: "Slight rain",
    63: "Moderate rain",
    65: "Heavy rain",
    66: "Light freezing rain",
    67: "Heavy freezing rain",
    71: "Slight snow fall",
    73: "Moderate snow fall",
    75: "Heavy snow fall",
    77: "Snow grains",
    80: "Slight rain showers",
    81: "Moderate rain showers",
    82: "Violent rain showers",
    85: "Slight snow showers",
    86: "Heavy snow showers",
    95: "Thunderstorm",
    96: "Thunderstorm with slight hail",
    99: "Thunderstorm with heavy hail",
}


def connect_consumer(retries: int = 10, delay: float = 5.0) -> KafkaConsumer:
    for attempt in range(1, retries + 1):
        try:
            return KafkaConsumer(
                KAFKA_TOPIC_RAW,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                group_id="weatherstream-processing",
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
    current = record.get("current", {})
    weather_code = current.get("weather_code")
    temperature_c = current.get("temperature_2m")
    wind_speed_kmh = current.get("wind_speed_10m")
    precipitation_mm = current.get("precipitation")

    is_extreme = False
    if temperature_c is not None and (temperature_c >= EXTREME_TEMP_HIGH_C or temperature_c <= EXTREME_TEMP_LOW_C):
        is_extreme = True
    if wind_speed_kmh is not None and wind_speed_kmh >= EXTREME_WIND_KMH:
        is_extreme = True
    if precipitation_mm is not None and precipitation_mm >= EXTREME_PRECIPITATION_MM:
        is_extreme = True

    return {
        "city": record["city"],
        "observed_at": current.get("time"),
        "temperature_c": temperature_c,
        "feels_like_c": current.get("apparent_temperature"),
        "humidity_pct": current.get("relative_humidity_2m"),
        "precipitation_mm": precipitation_mm,
        "wind_speed_kmh": wind_speed_kmh,
        "wind_direction_deg": current.get("wind_direction_10m"),
        "weather_code": weather_code,
        "weather_description": WMO_WEATHER_CODES.get(weather_code, "Unknown"),
        "is_extreme": is_extreme,
    }


def store_raw(conn, record: dict):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO raw_weather (city, latitude, longitude, observed_at, raw_payload)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (
                record["city"],
                record["latitude"],
                record["longitude"],
                record["current"].get("time"),
                json.dumps(record),
            ),
        )


def store_processed(conn, processed: dict):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO weather_processed (
                city, observed_at, temperature_c, feels_like_c, humidity_pct,
                precipitation_mm, wind_speed_kmh, wind_direction_deg,
                weather_code, weather_description, is_extreme
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (city, observed_at) DO UPDATE SET
                temperature_c = EXCLUDED.temperature_c,
                feels_like_c = EXCLUDED.feels_like_c,
                humidity_pct = EXCLUDED.humidity_pct,
                precipitation_mm = EXCLUDED.precipitation_mm,
                wind_speed_kmh = EXCLUDED.wind_speed_kmh,
                wind_direction_deg = EXCLUDED.wind_direction_deg,
                weather_code = EXCLUDED.weather_code,
                weather_description = EXCLUDED.weather_description,
                is_extreme = EXCLUDED.is_extreme,
                processed_at = now()
            """,
            (
                processed["city"],
                processed["observed_at"],
                processed["temperature_c"],
                processed["feels_like_c"],
                processed["humidity_pct"],
                processed["precipitation_mm"],
                processed["wind_speed_kmh"],
                processed["wind_direction_deg"],
                processed["weather_code"],
                processed["weather_description"],
                processed["is_extreme"],
            ),
        )

        if processed["is_extreme"]:
            cur.execute(
                """
                INSERT INTO weather_alerts (city, observed_at, alert_type, detail)
                VALUES (%s, %s, %s, %s)
                """,
                (
                    processed["city"],
                    processed["observed_at"],
                    "extreme_weather",
                    f"temp={processed['temperature_c']}C wind={processed['wind_speed_kmh']}km/h "
                    f"precip={processed['precipitation_mm']}mm",
                ),
            )


def run():
    consumer = connect_consumer()
    conn = connect_postgres()
    log.info("Connected to Kafka and Postgres. Waiting for messages on '%s'...", KAFKA_TOPIC_RAW)

    for message in consumer:
        record = message.value
        try:
            processed = transform(record)
            store_raw(conn, record)
            store_processed(conn, processed)
            flag = " [EXTREME]" if processed["is_extreme"] else ""
            log.info(
                "Stored %s @ %s: %.1fC, %s%s",
                processed["city"],
                processed["observed_at"],
                processed["temperature_c"] or float("nan"),
                processed["weather_description"],
                flag,
            )
        except Exception:
            log.exception("Failed to process record: %s", record)


if __name__ == "__main__":
    run()
