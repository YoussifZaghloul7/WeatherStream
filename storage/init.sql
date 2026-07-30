-- db schema for weatherstream, nothing too complicated

-- raw readings, just dumping the whole api response as json here too
-- in case i need something later that i didn't parse out
CREATE TABLE IF NOT EXISTS raw_weather (
    id             SERIAL PRIMARY KEY,
    city           VARCHAR(100) NOT NULL,
    latitude       DOUBLE PRECISION NOT NULL,
    longitude      DOUBLE PRECISION NOT NULL,
    observed_at    TIMESTAMP NOT NULL,
    ingested_at    TIMESTAMP NOT NULL DEFAULT now(),
    raw_payload    JSONB NOT NULL
);

-- cleaned up version, this is what the dashboard actually queries
CREATE TABLE IF NOT EXISTS weather_processed (
    id                    SERIAL PRIMARY KEY,
    city                  VARCHAR(100) NOT NULL,
    observed_at           TIMESTAMP NOT NULL,
    temperature_c         DOUBLE PRECISION,
    feels_like_c          DOUBLE PRECISION,
    humidity_pct          DOUBLE PRECISION,
    precipitation_mm      DOUBLE PRECISION,
    wind_speed_kmh        DOUBLE PRECISION,
    wind_direction_deg    DOUBLE PRECISION,
    weather_code          INT,
    weather_description   VARCHAR(100),
    is_extreme            BOOLEAN NOT NULL DEFAULT FALSE,
    processed_at          TIMESTAMP NOT NULL DEFAULT now(),
    UNIQUE (city, observed_at)
);

CREATE INDEX IF NOT EXISTS idx_weather_processed_city_time
    ON weather_processed (city, observed_at DESC);

-- gets a row whenever the consumer flags something as extreme weather
CREATE TABLE IF NOT EXISTS weather_alerts (
    id            SERIAL PRIMARY KEY,
    city          VARCHAR(100) NOT NULL,
    observed_at   TIMESTAMP NOT NULL,
    alert_type    VARCHAR(50) NOT NULL,
    detail        TEXT,
    created_at    TIMESTAMP NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_weather_alerts_city_time
    ON weather_alerts (city, observed_at DESC);

-- traffic readings, cairo only. no clean "observed_at" from the api like
-- weather has, so just stamping it with when we polled it
CREATE TABLE IF NOT EXISTS traffic_processed (
    id                     SERIAL PRIMARY KEY,
    location               VARCHAR(100) NOT NULL,
    current_speed_kmh      DOUBLE PRECISION,
    free_flow_speed_kmh    DOUBLE PRECISION,
    congestion_pct         DOUBLE PRECISION,
    road_closure           BOOLEAN NOT NULL DEFAULT FALSE,
    polled_at              TIMESTAMP NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_traffic_processed_location_time
    ON traffic_processed (location, polled_at DESC);
