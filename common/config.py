import os

from dotenv import load_dotenv

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC_RAW = os.getenv("KAFKA_TOPIC_RAW", "weather-raw")

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.getenv("POSTGRES_DB", "weatherstream")
POSTGRES_USER = os.getenv("POSTGRES_USER", "weather")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "weather")

POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", "60"))

# just tracking Cairo and the North Coast for now, kept it small since it's a solo project
CITIES = [
    ("Cairo", 30.0444, 31.2357),
    ("North Coast", 30.8481, 28.9642),  # roughly El Alamein / Marina area
]

# Thresholds used by the processing consumer to flag extreme weather
EXTREME_TEMP_HIGH_C = 40.0
EXTREME_TEMP_LOW_C = 0.0
EXTREME_WIND_KMH = 60.0
EXTREME_PRECIPITATION_MM = 10.0


def postgres_dsn() -> str:
    return (
        f"host={POSTGRES_HOST} port={POSTGRES_PORT} "
        f"dbname={POSTGRES_DB} user={POSTGRES_USER} password={POSTGRES_PASSWORD}"
    )


def postgres_sqlalchemy_url() -> str:
    return (
        f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}"
        f"@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
    )
