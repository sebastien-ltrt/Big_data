"""
Data Warehouse — chargement dans PostgreSQL via psycopg2.
"""
import os
import logging
import pandas as pd
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)


def get_connection():
    return psycopg2.connect(
        host=os.getenv("PG_HOST", "localhost"),
        port=int(os.getenv("PG_PORT", 5432)),
        dbname=os.getenv("PG_DB", "parkings_rennes"),
        user=os.getenv("PG_USER", "parking_user"),
        password=os.getenv("PG_PASSWORD", ""),
    )


def upsert_parkings(df: pd.DataFrame) -> int:
    """Upsert la table de dimension parkings."""
    sql = """
        INSERT INTO parkings (parking_id, name, source, type, lat, lon, total_spaces, address, city, last_seen)
        VALUES (%(parking_id)s, %(name)s, %(source)s, %(type)s, %(lat)s, %(lon)s,
                %(total_spaces)s, %(address)s, %(city)s, %(snapshot_time)s)
        ON CONFLICT (parking_id) DO UPDATE SET
            name         = EXCLUDED.name,
            total_spaces = EXCLUDED.total_spaces,
            last_seen    = EXCLUDED.last_seen;
    """
    cols = ["parking_id", "name", "source", "type", "lat", "lon",
            "total_spaces", "address", "city", "snapshot_time"]
    records = df[cols].to_dict("records")
    with get_connection() as conn, conn.cursor() as cur:
        psycopg2.extras.execute_batch(cur, sql, records)
    logger.info("Warehouse — upsert parkings : %d lignes", len(records))
    return len(records)


def insert_availability(df: pd.DataFrame) -> int:
    """Insère un snapshot de disponibilité."""
    cols = [
        "parking_id", "snapshot_time", "free_spaces", "occupied_spaces",
        "total_spaces", "occupancy_rate", "is_open", "is_critical", "is_full",
        "status", "free_ev", "free_carpool", "free_pmr",
        "temperature_c", "humidity_pct", "wind_speed_kmh", "weather_description",
    ]
    sql = """
        INSERT INTO availability_snapshots
            (parking_id, snapshot_time, free_spaces, occupied_spaces, total_spaces,
             occupancy_rate, is_open, is_critical, is_full, status,
             free_ev, free_carpool, free_pmr,
             temperature_c, humidity_pct, wind_speed_kmh, weather_desc)
        VALUES
            (%(parking_id)s, %(snapshot_time)s, %(free_spaces)s, %(occupied_spaces)s,
             %(total_spaces)s, %(occupancy_rate)s, %(is_open)s, %(is_critical)s,
             %(is_full)s, %(status)s, %(free_ev)s, %(free_carpool)s, %(free_pmr)s,
             %(temperature_c)s, %(humidity_pct)s, %(wind_speed_kmh)s, %(weather_description)s);
    """
    records = df[cols].to_dict("records")
    with get_connection() as conn, conn.cursor() as cur:
        psycopg2.extras.execute_batch(cur, sql, records)
    logger.info("Warehouse — %d snapshots insérés", len(records))
    return len(records)


def insert_weather(weather: dict) -> None:
    sql = """
        INSERT INTO weather_snapshots
            (scraped_at, temperature_c, humidity_pct, wind_speed_kmh,
             wind_direction, weather_description, scrape_error)
        VALUES (%(scraped_at)s, %(temperature_c)s, %(humidity_pct)s, %(wind_speed_kmh)s,
                %(wind_direction)s, %(weather_description)s, %(scrape_error)s);
    """
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(sql, weather)


def load_parkings_df() -> pd.DataFrame:
    """Charge les dernières disponibilités par parking."""
    sql = """
        SELECT p.parking_id, p.name, p.source, p.type, p.lat, p.lon,
               p.total_spaces, p.address, p.city,
               a.free_spaces, a.occupied_spaces, a.occupancy_rate,
               a.is_open, a.is_critical, a.is_full, a.status,
               a.free_ev, a.free_carpool, a.free_pmr,
               a.snapshot_time, a.temperature_c, a.humidity_pct,
               a.wind_speed_kmh, a.weather_desc AS weather_description
        FROM parkings p
        LEFT JOIN LATERAL (
            SELECT * FROM availability_snapshots
            WHERE parking_id = p.parking_id
            ORDER BY snapshot_time DESC LIMIT 1
        ) a ON TRUE;
    """
    with get_connection() as conn:
        return pd.read_sql(sql, conn)


def load_availability_history(hours: int = 24) -> pd.DataFrame:
    sql = f"""
        SELECT parking_id, snapshot_time, free_spaces,
               occupied_spaces, occupancy_rate, temperature_c
        FROM availability_snapshots
        WHERE snapshot_time > NOW() - INTERVAL '{hours} hours'
        ORDER BY snapshot_time;
    """
    with get_connection() as conn:
        return pd.read_sql(sql, conn)


def load_weather_history(hours: int = 24) -> pd.DataFrame:
    sql = f"""
        SELECT scraped_at, temperature_c, humidity_pct,
               wind_speed_kmh, weather_description
        FROM weather_snapshots
        WHERE scraped_at > NOW() - INTERVAL '{hours} hours'
          AND scrape_error = FALSE
        ORDER BY scraped_at;
    """
    with get_connection() as conn:
        return pd.read_sql(sql, conn)
