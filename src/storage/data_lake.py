"""
Data Lake local — sauvegarde append-only des snapshots bruts dans data/raw/.
Chaque fichier est horodaté : source_YYYYMMDD_HHMMSS.json
"""
import json
import re
import logging
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

RAW_DIR       = Path("data/raw")
PROCESSED_DIR = Path("data/processed")


def _ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


def save_raw(data, source: str) -> Path:
    """Sauvegarde un snapshot brut (dict ou list) dans data/raw/<source>/."""
    dest = RAW_DIR / source
    dest.mkdir(parents=True, exist_ok=True)
    path = dest / f"{source}_{_ts()}.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    logger.info("Data Lake [%s] → %s", source, path)
    return path


def save_processed(df: pd.DataFrame, name: str = "latest") -> Path:
    """Sauvegarde le DataFrame transformé en CSV dans data/processed/."""
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    path = PROCESSED_DIR / f"{name}.csv"
    df.to_csv(path, index=False, encoding="utf-8")
    return path


def _filename_to_dt(filename: str) -> datetime | None:
    m = re.search(r"(\d{8}_\d{6})", filename)
    if not m:
        return None
    try:
        return datetime.strptime(m.group(1), "%Y%m%d_%H%M%S").replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def load_latest_raw(source: str):
    """Charge le dernier snapshot d'une source depuis le Data Lake."""
    files = sorted((RAW_DIR / source).glob(f"{source}_*.json"))
    if not files:
        return None
    with open(files[-1], encoding="utf-8") as f:
        return json.load(f)


def load_weather_history(hours: int = 24) -> pd.DataFrame:
    """Construit un historique météo depuis les fichiers du Data Lake."""
    files = sorted((RAW_DIR / "weather").glob("weather_*.json"))
    rows = []
    for f in files:
        dt = _filename_to_dt(f.name)
        if dt is None:
            continue
        if (datetime.now(timezone.utc) - dt).total_seconds() / 3600 > hours:
            continue
        with open(f, encoding="utf-8") as fp:
            w = json.load(fp)
        if w.get("scrape_error"):
            continue
        rows.append({
            "scraped_at":          dt,
            "temperature_c":       w.get("temperature_c"),
            "humidity_pct":        w.get("humidity_pct"),
            "wind_speed_kmh":      w.get("wind_speed_kmh"),
            "weather_description": w.get("weather_description"),
        })
    return pd.DataFrame(rows) if rows else pd.DataFrame()


def load_parking_history(hours: int = 24) -> pd.DataFrame:
    """Construit un historique de disponibilité parkings depuis le Data Lake."""
    rows = []
    for source in ["citedia", "star"]:
        files = sorted((RAW_DIR / source).glob(f"{source}_*.json"))
        for f in files:
            dt = _filename_to_dt(f.name)
            if dt is None:
                continue
            if (datetime.now(timezone.utc) - dt).total_seconds() / 3600 > hours:
                continue
            with open(f, encoding="utf-8") as fp:
                data = json.load(fp)
            # Citedia : liste de dicts
            if source == "citedia" and isinstance(data, list):
                for p in data:
                    rows.append({
                        "snapshot_time":  dt,
                        "source":         "citedia",
                        "parking_id":     p.get("id"),
                        "name":           p.get("name"),
                        "free":           p.get("free", 0),
                        "max":            p.get("max", 0),
                    })
            # STAR : dict avec "realtime"
            elif source == "star" and isinstance(data, dict):
                for p in data.get("realtime", []):
                    rows.append({
                        "snapshot_time":  dt,
                        "source":         "star",
                        "parking_id":     p.get("idparc"),
                        "name":           p.get("nom"),
                        "free":           p.get("jrdinfosoliste", 0),
                        "max":            p.get("capacitesoliste", 0),
                    })
    return pd.DataFrame(rows) if rows else pd.DataFrame()
