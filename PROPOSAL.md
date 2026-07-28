## Project Name
WeatherStream

## Team Members
Youssef Zaghloul

## Project Description
WeatherStream is a real-time weather data pipeline. It collects live
weather data for Cairo and the North Coast from a free public API. The
data moves through Kafka, gets cleaned, and is saved in a PostgreSQL
database. A live dashboard shows temperature, humidity, and wind charts,
and gives an alert when the weather is extreme.

I chose only two cities to keep the project small and finish it well.

## Tools
- Python for the data scripts
- Apache Kafka for streaming
- PostgreSQL for storage
- Streamlit and Plotly for the dashboard
- Docker Compose to run everything
- GitHub for version control

## Workflow
1. A Python script checks the Open-Meteo API every minute for Cairo and
   the North Coast, and sends each reading to Kafka.
2. Kafka holds and moves the messages.
3. Another Python script reads the messages from Kafka, cleans the data,
   adds a weather description and an extreme weather flag, and saves it
   in PostgreSQL.
4. PostgreSQL stores both the raw data and the cleaned data.
5. The dashboard reads from PostgreSQL and shows live charts that update
   on their own.

```
Open-Meteo API -> Producer -> Kafka -> Consumer (ETL) -> PostgreSQL -> Dashboard
```

GitHub: https://github.com/YoussifZaghloul7/WeatherStream
