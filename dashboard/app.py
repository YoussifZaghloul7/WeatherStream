# dashboard - just reads from postgres and plots it. streamlit handles
# the refresh loop for us so it feels live

import os
import sys

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from sqlalchemy import create_engine
from streamlit_autorefresh import st_autorefresh

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common.config import postgres_sqlalchemy_url

st.set_page_config(page_title="WeatherStream", page_icon="\U0001F326", layout="wide")

st_autorefresh(interval=30_000, key="autorefresh")


@st.cache_resource
def get_engine():
    return create_engine(postgres_sqlalchemy_url())


@st.cache_data(ttl=25)
def load_processed(hours: int) -> pd.DataFrame:
    query = f"""
        SELECT city, observed_at, temperature_c, feels_like_c, humidity_pct,
               precipitation_mm, wind_speed_kmh, wind_direction_deg,
               weather_description, is_extreme
        FROM weather_processed
        WHERE observed_at >= now() - interval '{hours} hours'
        ORDER BY observed_at ASC
    """
    return pd.read_sql(query, get_engine())


@st.cache_data(ttl=25)
def load_traffic(hours: int) -> pd.DataFrame:
    query = f"""
        SELECT location, current_speed_kmh, free_flow_speed_kmh, congestion_pct, polled_at
        FROM traffic_processed
        WHERE polled_at >= now() - interval '{hours} hours'
        ORDER BY polled_at ASC
    """
    return pd.read_sql(query, get_engine())


@st.cache_data(ttl=25)
def load_alerts(limit: int = 20) -> pd.DataFrame:
    query = f"""
        SELECT city, observed_at, alert_type, detail, created_at
        FROM weather_alerts
        ORDER BY created_at DESC
        LIMIT {limit}
    """
    return pd.read_sql(query, get_engine())


st.title("\U0001F326 WeatherStream — Real-Time Weather Dashboard")
st.caption("Open-Meteo API -> Kafka -> ETL -> PostgreSQL -> Streamlit  |  auto-refreshes every 30s")

hours_back = st.sidebar.slider("Time window (hours)", min_value=1, max_value=72, value=24)
df = load_processed(hours_back)

if df.empty:
    st.warning(
        "No data yet. Make sure the producer and consumer are running "
        "(`python ingestion/producer.py` and `python processing/consumer.py`)."
    )
    st.stop()

cities = sorted(df["city"].unique())
selected_cities = st.sidebar.multiselect("Cities", cities, default=cities)
df = df[df["city"].isin(selected_cities)]

latest = df.sort_values("observed_at").groupby("city").tail(1).set_index("city")

st.subheader("Current conditions")
cols = st.columns(len(latest)) if len(latest) else [st]
for col, (city, row) in zip(cols, latest.iterrows()):
    with col:
        st.metric(city, f"{row['temperature_c']:.1f} °C", f"feels {row['feels_like_c']:.1f} °C")
        st.caption(f"{row['weather_description']} · {row['humidity_pct']:.0f}% humidity · {row['wind_speed_kmh']:.0f} km/h wind")
        if row["is_extreme"]:
            st.error("Extreme weather flagged")

st.subheader("Temperature over time")
fig_temp = px.line(df, x="observed_at", y="temperature_c", color="city", markers=True)
st.plotly_chart(fig_temp, use_container_width=True)

col_a, col_b = st.columns(2)
with col_a:
    st.subheader("Humidity over time")
    fig_hum = px.line(df, x="observed_at", y="humidity_pct", color="city")
    st.plotly_chart(fig_hum, use_container_width=True)
with col_b:
    st.subheader("Wind speed over time")
    fig_wind = px.line(df, x="observed_at", y="wind_speed_kmh", color="city")
    st.plotly_chart(fig_wind, use_container_width=True)

st.subheader("City comparison (latest reading)")
fig_bar = px.bar(latest.reset_index(), x="city", y="temperature_c", color="city")
st.plotly_chart(fig_bar, use_container_width=True)

st.subheader("Traffic vs weather (Cairo)")
traffic_df = load_traffic(hours_back)
cairo_weather = df[df["city"] == "Cairo"]
if traffic_df.empty:
    st.info(
        "No traffic data yet. Set TOMTOM_API_KEY in .env and run "
        "`python ingestion/traffic_producer.py` and `python processing/traffic_consumer.py`."
    )
else:
    fig_combo = go.Figure()
    fig_combo.add_trace(go.Scatter(
        x=cairo_weather["observed_at"], y=cairo_weather["temperature_c"],
        name="Temperature (°C)", yaxis="y1", mode="lines+markers",
        line=dict(color="#FF8C42"), marker=dict(color="#FF8C42"),
    ))
    fig_combo.add_trace(go.Scatter(
        x=traffic_df["polled_at"], y=traffic_df["congestion_pct"],
        name="Traffic congestion (%)", yaxis="y2", mode="lines+markers",
        line=dict(color="#4A90D9"), marker=dict(color="#4A90D9"),
    ))
    fig_combo.update_layout(
        xaxis=dict(title="Time"),
        yaxis=dict(title="Temperature (°C)"),
        yaxis2=dict(title="Congestion (%)", overlaying="y", side="right"),
        legend=dict(orientation="h", y=1.1),
    )
    st.plotly_chart(fig_combo, use_container_width=True)

st.subheader("⚠️ Extreme weather alerts")
alerts = load_alerts()
if alerts.empty:
    st.info("No extreme weather alerts recorded yet.")
else:
    st.dataframe(alerts, use_container_width=True)

with st.expander("Raw processed data"):
    st.dataframe(df, use_container_width=True)
