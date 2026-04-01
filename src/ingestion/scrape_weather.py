"""
Scraping — Météo de Rennes via wttr.in
Page HTML parsée avec BeautifulSoup + appel JSON pour compléter.
"""
import os
import re
import sys
import logging
from datetime import datetime, timezone
from pathlib import Path

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from src.storage.data_lake import save_raw

load_dotenv()
logger = logging.getLogger(__name__)

WEATHER_URL = os.getenv("WEATHER_URL", "https://wttr.in/Rennes")
HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/124.0 Safari/537.36",
    "Accept-Language": "fr-FR,fr;q=0.9",
}
ARROW_TO_DIR = {
    "↑": "N", "↗": "NE", "→": "E", "↘": "SE",
    "↓": "S", "↙": "SO", "←": "O", "↖": "NO",
}


def scrape_weather() -> dict:
    """Scrape wttr.in/Rennes et retourne les conditions météo actuelles."""
    result = {
        "scraped_at": datetime.now(timezone.utc).isoformat(),
        "scrape_error": False,
        "temperature_c": None,
        "humidity_pct": None,
        "wind_speed_kmh": None,
        "wind_direction": None,
        "weather_description": None,
    }
    try:
        # --- Scraping HTML avec BeautifulSoup ---
        resp = requests.get(WEATHER_URL, headers=HEADERS, timeout=15)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "lxml")
        pre = soup.find("pre")
        if not pre:
            raise ValueError("Balise <pre> introuvable sur wttr.in")
        text = pre.get_text()

        # Température
        m = re.search(r"[+\-]?(\d+)\(?[\d\-]*\)?\s*°C", text)
        if m:
            result["temperature_c"] = float(m.group(1))

        # Vent
        m = re.search(r"([\u2190-\u2199↑↓←→↗↘↙↖]?)\s*(\d+)\s*km/h", text)
        if m:
            result["wind_speed_kmh"] = float(m.group(2))
            result["wind_direction"] = ARROW_TO_DIR.get(m.group(1).strip()) or m.group(1).strip() or None

        # Description
        lines = [l.strip() for l in text.splitlines() if l.strip()]
        for i, line in enumerate(lines):
            if "Weather report" in line and i + 1 < len(lines):
                result["weather_description"] = lines[i + 1]
                break

        # --- Compléter avec l'API JSON wttr.in (humidité + vérification) ---
        json_resp = requests.get(
            "https://wttr.in/Rennes?format=j1", headers=HEADERS, timeout=10
        )
        if json_resp.ok:
            current = json_resp.json()["current_condition"][0]
            result["humidity_pct"] = int(current.get("humidity", 0)) or None
            if result["temperature_c"] is None:
                result["temperature_c"] = float(current.get("temp_C", 0))
            if result["wind_speed_kmh"] is None:
                result["wind_speed_kmh"] = float(current.get("windspeedKmph", 0))
            if result["wind_direction"] is None:
                result["wind_direction"] = current.get("winddir16Point")
            desc = current.get("weatherDesc", [{}])[0].get("value")
            if desc:
                result["weather_description"] = desc

        logger.info(
            "Scraping OK — %s°C | %s%% | %s km/h | %s",
            result["temperature_c"], result["humidity_pct"],
            result["wind_speed_kmh"], result["weather_description"],
        )
    except Exception as exc:
        logger.warning("Scraping échoué : %s", exc)
        result["scrape_error"] = True

    return result


def run_scraping() -> dict:
    """Scrape la météo et sauvegarde dans le Data Lake."""
    weather = scrape_weather()
    save_raw(weather, source="weather")
    return weather
