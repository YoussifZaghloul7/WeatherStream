## Project Name
WeatherStream

## Team Members
Youssef Zaghloul (solo)

## Project Description
A real-time weather data pipeline. It pulls live weather readings for
Cairo and the North Coast from a free public API, streams them through
Kafka, cleans/transforms the data, stores it in PostgreSQL, and displays
it on a live dashboard with temperature/humidity/wind trends and an alert
for extreme weather conditions.

Kept the scope to 2 locations since I'm working on this alone and wanted
something I could actually finish well instead of something half-done.

## Tools
- Python (ingestion + ETL scripts)
- Apache Kafka (streaming, Dockerized)
- PostgreSQL (storage)
- Streamlit + Plotly (dashboard)
- Docker Compose (runs everything locally)
- GitHub

## Workflow
1. Producer script polls the Open-Meteo API every minute for Cairo and the
   North Coast, sends each reading to Kafka as JSON.
2. Kafka buffers/streams the messages.
3. Consumer script reads from Kafka, cleans and transforms the data
   (readable weather descriptions, extreme weather flags), writes it to
   Postgres.
4. Postgres holds both raw and processed tables.
5. Dashboard reads from Postgres and shows live charts, refreshing
   automatically.

```
Open-Meteo API -> Producer -> Kafka -> Consumer (ETL) -> Postgres -> Dashboard
```

GitHub: https://github.com/YoussifZaghloul7/WeatherStream
