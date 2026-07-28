# WeatherStream

Real-time weather pipeline I built for the NTI Big Data course project.
Pulls live weather for Cairo and the North Coast, pushes it through Kafka,
cleans it up, dumps it into Postgres, and shows it on a dashboard.

Solo project - Youssef Zaghloul.

I picked weather data because it's free, updates constantly, and it's easy
to explain in a demo without needing some huge dataset nobody's seen before.
Only tracking 2 locations (Cairo + North Coast) since it's just me working
on this, didn't want to overscope it.

## How the pipeline works

```
Open-Meteo API  -->  producer.py  -->  Kafka (weather-raw)  -->  consumer.py  -->  Postgres  -->  Streamlit
```

1. `ingestion/producer.py` hits the Open-Meteo API (free, no key needed)
   every 60 seconds for Cairo and the North Coast, and pushes each reading
   into Kafka as a JSON message.
2. Kafka just sits in the middle and buffers everything - this is the
   "streaming" part of the pipeline.
3. `processing/consumer.py` reads from Kafka, cleans the data up a bit
   (converts weather codes to readable text, checks if anything counts as
   extreme weather), and writes it to Postgres.
4. Postgres stores both the raw data and the cleaned version.
5. `dashboard/app.py` (Streamlit) reads straight from Postgres and shows
   live charts. Refreshes every 30s on its own.

There's also Kafdrop running in Docker so you can actually see the messages
sitting in the Kafka topic, useful for the demo.

## Tools used

- Python (ingestion + processing scripts)
- Apache Kafka - streaming
- PostgreSQL - storage
- Streamlit + Plotly - dashboard
- Docker Compose - runs Kafka/Postgres/Kafdrop so I don't have to install
  any of that stuff natively
- GitHub obviously

## Running it

Start the infra first:

```bash
docker compose up -d
```

give it like 15 seconds to actually come up, then:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

then 3 terminals:

```bash
# 1
python ingestion/producer.py

# 2
python processing/consumer.py

# 3
streamlit run dashboard/app.py
```

dashboard shows up at localhost:8501, Kafdrop is at localhost:9001.

to stop everything: `docker compose down`

## Project layout

```
WeatherStream/
├── docker-compose.yml      -> kafka, postgres, kafdrop
├── requirements.txt
├── common/config.py        -> cities + settings live here
├── ingestion/producer.py   -> pulls weather, sends to kafka
├── processing/consumer.py  -> etl, writes to postgres
├── storage/init.sql        -> db schema
└── dashboard/app.py        -> the actual dashboard
```

## Notes / things I'd add if I had more time

- forecast data, not just current conditions, so you could see trends
  instead of just live numbers
- a second Kafka topic for the cleaned data so other stuff could consume it
- maybe hosting the dashboard somewhere so it's not just localhost
