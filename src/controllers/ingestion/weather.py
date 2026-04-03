"""
Controller — Météo Rennes via wttr.in
Utilise l'API JSON (plus fiable) en priorité, scraping HTML en fallback.
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

WEATHER_URL = os.getenv("WEATHER_URL", "https://wttr.in/Rennes")
HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/124.0 Safari/537.36",
    "Accept-Language": "fr-FR,fr;q=0.9",
}


def scrape_weather() -> dict:
    """Récupère la météo Rennes. Priorité : API JSON wttr.in, fallback HTML."""
    result = {
        "scraped_at":          datetime.now(timezone.utc).isoformat(),
        "scrape_error":        False,
        "temperature_c":       None,
        "humidity_pct":        None,
        "wind_speed_kmh":      None,
        "wind_direction":      None,
        "weather_description": None,
    }

    # ── Tentative 1 : API JSON (priorité) ─────────────────────────────────────
    try:
        json_resp = requests.get(
            f"{WEATHER_URL}?format=j1", headers=HEADERS, timeout=10
        )
        if json_resp.ok:
            current = json_resp.json()["current_condition"][0]
            result["temperature_c"]       = float(current.get("temp_C") or 0)
            result["humidity_pct"]        = int(current.get("humidity") or 0) or None
            result["wind_speed_kmh"]      = float(current.get("windspeedKmph") or 0)
            result["wind_direction"]      = current.get("winddir16Point")
            desc_list = current.get("weatherDesc") or [{}]
            result["weather_description"] = desc_list[0].get("value") if desc_list else None
            logger.info(
                "Météo JSON OK — %s°C | %s%% | %s km/h | %s",
                result["temperature_c"], result["humidity_pct"],
                result["wind_speed_kmh"], result["weather_description"],
            )
            return result
    except Exception as exc:
        logger.warning("wttr.in JSON échoué : %s", exc)

    # ── Tentative 2 : fallback Open-Meteo (aucune clé requise) ────────────────
    try:
        r = requests.get(
            "https://api.open-meteo.com/v1/forecast",
            params={
                "latitude": 48.1085,
                "longitude": -1.6772,
                "current": "temperature_2m,relative_humidity_2m,wind_speed_10m,weather_code",
                "wind_speed_unit": "kmh",
                "timezone": "Europe/Paris",
            },
            timeout=10,
        )
        if r.ok:
            cur = r.json().get("current", {})
            wcode = int(cur.get("weather_code", 0))
            WMO = {
                0: "Ciel dégagé", 1: "Principalement dégagé", 2: "Partiellement nuageux",
                3: "Nuageux", 45: "Brouillard", 48: "Brouillard givrant",
                51: "Bruine légère", 53: "Bruine", 55: "Bruine dense",
                61: "Pluie légère", 63: "Pluie", 65: "Pluie forte",
                71: "Neige légère", 73: "Neige", 75: "Neige forte",
                80: "Averses légères", 81: "Averses", 82: "Averses violentes",
                95: "Orage", 96: "Orage avec grêle", 99: "Orage violent",
            }
            result["temperature_c"]       = float(cur.get("temperature_2m") or 0)
            result["humidity_pct"]        = int(cur.get("relative_humidity_2m") or 0) or None
            result["wind_speed_kmh"]      = float(cur.get("wind_speed_10m") or 0)
            result["weather_description"] = WMO.get(wcode, f"Code {wcode}")
            logger.info(
                "Météo Open-Meteo OK — %s°C | %s%% | %s km/h | %s",
                result["temperature_c"], result["humidity_pct"],
                result["wind_speed_kmh"], result["weather_description"],
            )
            return result
    except Exception as exc:
        logger.warning("Open-Meteo échoué : %s", exc)

    result["scrape_error"] = True
    return result


def run_scraping() -> dict:
    """Récupère la météo et sauvegarde dans le Data Lake."""
    weather = scrape_weather()
    save_raw(weather, source="weather")
    return weather
