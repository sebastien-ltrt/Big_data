"""
Controller — Météo Rennes via Open-Meteo (API publique, sans clé).
Fallback : wttr.in JSON.
"""
import os
import sys
import logging
from datetime import datetime, timezone
from pathlib import Path

import requests
from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
from src.models.data_lake import save_raw

load_dotenv()
logger = logging.getLogger(__name__)

HEADERS = {"User-Agent": "ParkingsRennes/1.0"}

# Codes WMO → description lisible
WMO_CODES = {
    0: "Ciel dégagé", 1: "Principalement dégagé", 2: "Partiellement nuageux",
    3: "Nuageux", 45: "Brouillard", 48: "Brouillard givrant",
    51: "Bruine légère", 53: "Bruine", 55: "Bruine dense",
    61: "Pluie légère", 63: "Pluie", 65: "Pluie forte",
    71: "Neige légère", 73: "Neige", 75: "Neige forte",
    80: "Averses légères", 81: "Averses", 82: "Averses violentes",
    95: "Orage", 96: "Orage avec grêle", 99: "Orage violent",
}


def _from_open_meteo() -> dict | None:
    """Open-Meteo — API gratuite, sans clé, très fiable."""
    try:
        r = requests.get(
            "https://api.open-meteo.com/v1/forecast",
            params={
                "latitude": 48.1085,
                "longitude": -1.6772,
                "current": "temperature_2m,relative_humidity_2m,wind_speed_10m,wind_direction_10m,weather_code",
                "wind_speed_unit": "kmh",
                "timezone": "Europe/Paris",
            },
            timeout=10,
        )
        r.raise_for_status()
        cur = r.json()["current"]
        wcode = int(cur.get("weather_code", 0))
        result = {
            "temperature_c":       round(float(cur["temperature_2m"]), 1),
            "humidity_pct":        int(cur["relative_humidity_2m"]),
            "wind_speed_kmh":      round(float(cur["wind_speed_10m"]), 1),
            "wind_direction":      str(int(cur.get("wind_direction_10m", 0))) + "°",
            "weather_description": WMO_CODES.get(wcode, f"Code {wcode}"),
        }
        logger.info(
            "Open-Meteo OK — %s°C | %s%% | %s km/h | %s",
            result["temperature_c"], result["humidity_pct"],
            result["wind_speed_kmh"], result["weather_description"],
        )
        return result
    except Exception as exc:
        logger.warning("Open-Meteo échoué : %s", exc)
        return None


def _from_wttr() -> dict | None:
    """wttr.in JSON — fallback."""
    try:
        r = requests.get(
            "https://wttr.in/Rennes?format=j1",
            headers=HEADERS,
            timeout=10,
        )
        r.raise_for_status()
        cur = r.json()["current_condition"][0]
        desc_list = cur.get("weatherDesc") or [{}]
        desc = desc_list[0].get("value", "") if desc_list else ""
        result = {
            "temperature_c":       float(cur.get("temp_C") or 0),
            "humidity_pct":        int(cur.get("humidity") or 0) or None,
            "wind_speed_kmh":      float(cur.get("windspeedKmph") or 0),
            "wind_direction":      cur.get("winddir16Point"),
            "weather_description": desc or None,
        }
        logger.info(
            "wttr.in OK — %s°C | %s%% | %s km/h | %s",
            result["temperature_c"], result["humidity_pct"],
            result["wind_speed_kmh"], result["weather_description"],
        )
        return result
    except Exception as exc:
        logger.warning("wttr.in échoué : %s", exc)
        return None


def scrape_weather() -> dict:
    result = {
        "scraped_at":          datetime.now(timezone.utc).isoformat(),
        "scrape_error":        False,
        "temperature_c":       None,
        "humidity_pct":        None,
        "wind_speed_kmh":      None,
        "wind_direction":      None,
        "weather_description": None,
    }
    data = _from_open_meteo() or _from_wttr()
    if data:
        result.update(data)
    else:
        result["scrape_error"] = True
        logger.error("Toutes les sources météo ont échoué.")
    return result


def run_scraping() -> dict:
    weather = scrape_weather()
    save_raw(weather, source="weather")
    return weather
