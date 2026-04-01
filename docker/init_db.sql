-- Créé automatiquement au démarrage du conteneur postgres-parkings

CREATE TABLE IF NOT EXISTS parkings (
    parking_id   VARCHAR(20)      PRIMARY KEY,
    name         VARCHAR(200)     NOT NULL,
    source       VARCHAR(20)      NOT NULL,
    type         VARCHAR(30),
    lat          DOUBLE PRECISION,
    lon          DOUBLE PRECISION,
    total_spaces INTEGER          DEFAULT 0,
    address      VARCHAR(200),
    city         VARCHAR(100)     DEFAULT 'Rennes',
    last_seen    TIMESTAMPTZ      DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS availability_snapshots (
    id               BIGSERIAL    PRIMARY KEY,
    parking_id       VARCHAR(20)  NOT NULL REFERENCES parkings(parking_id),
    snapshot_time    TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    free_spaces      INTEGER      DEFAULT 0,
    occupied_spaces  INTEGER      DEFAULT 0,
    total_spaces     INTEGER      DEFAULT 0,
    occupancy_rate   NUMERIC(5,1),
    is_open          BOOLEAN      DEFAULT TRUE,
    is_critical      BOOLEAN      DEFAULT FALSE,
    is_full          BOOLEAN      DEFAULT FALSE,
    status           VARCHAR(30),
    free_ev          INTEGER,
    free_carpool     INTEGER,
    free_pmr         INTEGER,
    temperature_c    NUMERIC(4,1),
    humidity_pct     INTEGER,
    wind_speed_kmh   NUMERIC(5,1),
    weather_desc     VARCHAR(100)
);

CREATE INDEX IF NOT EXISTS idx_avail_parking_time ON availability_snapshots(parking_id, snapshot_time DESC);
CREATE INDEX IF NOT EXISTS idx_avail_time ON availability_snapshots(snapshot_time DESC);

CREATE TABLE IF NOT EXISTS weather_snapshots (
    id                  BIGSERIAL   PRIMARY KEY,
    scraped_at          TIMESTAMPTZ NOT NULL,
    temperature_c       NUMERIC(4,1),
    humidity_pct        INTEGER,
    wind_speed_kmh      NUMERIC(5,1),
    wind_direction      VARCHAR(20),
    weather_description VARCHAR(100),
    scrape_error        BOOLEAN     DEFAULT FALSE
);

CREATE INDEX IF NOT EXISTS idx_weather_time ON weather_snapshots(scraped_at DESC);
