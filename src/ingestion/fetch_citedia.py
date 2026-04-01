"""
Ingestion — API Citedia
10 parkings en ouvrage du centre-ville de Rennes.
Endpoints :
  GET /r1/parks        → liste + infos de chaque parking
  GET /r1/parks/status → disponibilité temps réel (plus rapide)
"""
import os
import sys
import logging
from datetime import datetime, timezone
from pathlib import Path

import requests
from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from src.storage.data_lake import save_raw

load_dotenv()
logger = logging.getLogger(__name__)

BASE_URL = os.getenv("CITEDIA_API_URL", "http://data.citedia.com/r1/parks")
HEADERS = {"Accept": "application/json"}


def fetch_parks_detail() -> list[dict]:
    """Récupère les infos complètes de chaque parking (nom, capacité, statut, dispo)."""
    resp = requests.get(BASE_URL, headers=HEADERS, timeout=15)
    resp.raise_for_status()
    data = resp.json()
    parks = []
    for item in data.get("parks", []):
        info = item.get("parkInformation", {})
        parks.append({
            "id":     item.get("id"),
            "name":   info.get("name"),
            "status": info.get("status"),
            "max":    info.get("max", 0),
            "free":   info.get("free", 0),
            "fetched_at": datetime.now(timezone.utc).isoformat(),
        })
    logger.info("Citedia — %d parkings récupérés", len(parks))
    return parks


def run_ingestion_citedia() -> list[dict]:
    """Récupère les parkings Citedia et sauvegarde dans le Data Lake."""
    parks = fetch_parks_detail()
    save_raw(parks, source="citedia")
    return parks
