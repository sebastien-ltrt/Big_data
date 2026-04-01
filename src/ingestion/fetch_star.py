"""
Ingestion — API STAR Rennes Métropole
Datasets :
  tco-parcsrelais-star-etat-tr      → disponibilité temps réel (8 parcs-relais P+R)
  tco-parcsrelais-star-topologie-td → topologie / adresses
"""
import os
import sys
import logging
from pathlib import Path

import requests
from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from src.storage.data_lake import save_raw

load_dotenv()
logger = logging.getLogger(__name__)

BASE_URL = os.getenv("STAR_API_BASE_URL", "https://data.explore.star.fr/api/explore/v2.1")
DATASET_REALTIME  = "tco-parcsrelais-star-etat-tr"
DATASET_TOPOLOGY  = "tco-parcsrelais-star-topologie-td"


def _fetch_dataset(dataset: str) -> list[dict]:
    """Récupère tous les enregistrements d'un dataset STAR (avec pagination)."""
    records, offset, limit = [], 0, 100
    api_key = os.getenv("STAR_API_KEY")
    headers = {"Authorization": f"Apikey {api_key}"} if api_key else {}
    url = f"{BASE_URL}/catalog/datasets/{dataset}/records"
    while True:
        resp = requests.get(url, params={"limit": limit, "offset": offset},
                            headers=headers, timeout=15)
        resp.raise_for_status()
        page = resp.json().get("results", [])
        records.extend(page)
        if len(page) < limit:
            break
        offset += limit
    logger.info("STAR [%s] — %d enregistrements", dataset, len(records))
    return records


def run_ingestion_star() -> dict:
    """Récupère les deux datasets STAR et sauvegarde dans le Data Lake."""
    realtime = _fetch_dataset(DATASET_REALTIME)
    topology = _fetch_dataset(DATASET_TOPOLOGY)
    snapshot = {"realtime": realtime, "topology": topology}
    save_raw(snapshot, source="star")
    return snapshot
