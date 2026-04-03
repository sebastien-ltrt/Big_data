"""
Data Lake MinIO — sauvegarde append-only des snapshots bruts dans MinIO (S3-compatible).
Chaque objet est horodaté : <source>/<source>_YYYYMMDD_HHMMSS.json
Rien n'est stocké sur le disque local.

Comportement si MinIO est inaccessible :
  - Toutes les fonctions d'écriture échouent silencieusement (log DEBUG).
  - Le pipeline n'est pas interrompu.
"""
import io
import json
import re
import logging
import os
from datetime import datetime, timezone
from functools import lru_cache

import urllib3
import pandas as pd
from minio import Minio
from minio.error import S3Error
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

# ── Configuration MinIO ────────────────────────────────────────────────────────
_ENDPOINT   = os.getenv("MINIO_ENDPOINT",   "localhost:9000")
_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
_SECURE     = os.getenv("MINIO_SECURE", "false").lower() == "true"

BUCKET_RAW       = os.getenv("MINIO_BUCKET_RAW",       "parkings-raw")
BUCKET_PROCESSED = os.getenv("MINIO_BUCKET_PROCESSED", "parkings-processed")


# ── Client MinIO (singleton léger) ────────────────────────────────────────────

def _client() -> Minio:
    """Retourne un client MinIO avec timeout court et zéro retry.

    Timeout : 1.5 s en connexion, 3 s en lecture.
    Retries  : désactivés — échoue immédiatement si MinIO est absent,
               ce qui évite les blocages de ~4 s en mode local sans Docker.
    """
    http = urllib3.PoolManager(
        timeout=urllib3.Timeout(connect=1.5, read=3.0),
        retries=urllib3.Retry(total=0, raise_on_redirect=False, read=False),
    )
    return Minio(
        _ENDPOINT,
        access_key=_ACCESS_KEY,
        secret_key=_SECRET_KEY,
        secure=_SECURE,
        http_client=http,
    )


def _ensure_bucket(client: Minio, bucket: str) -> None:
    """Crée le bucket s'il n'existe pas encore."""
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)
        logger.info("MinIO bucket créé : %s", bucket)


def _ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


# ── Écriture ───────────────────────────────────────────────────────────────────

def save_raw(data, source: str) -> str | None:
    """Upload un snapshot brut (dict ou list) dans MinIO sous <source>/<source>_ts.json.

    Retourne la clé de l'objet ou None si MinIO est indisponible (non bloquant).
    """
    try:
        client = _client()
        _ensure_bucket(client, BUCKET_RAW)

        key     = f"{source}/{source}_{_ts()}.json"
        content = json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8")
        buf     = io.BytesIO(content)

        client.put_object(
            BUCKET_RAW, key, buf, length=len(content), content_type="application/json"
        )
        logger.info("Data Lake MinIO [%s] -> %s/%s", source, BUCKET_RAW, key)
        return key
    except Exception as exc:
        logger.debug("Data Lake MinIO indisponible [%s] : %s", source, exc)
        return None


def save_processed(df: pd.DataFrame, name: str = "latest") -> str | None:
    """Upload le DataFrame transformé en CSV dans MinIO (bucket processed).

    Retourne la clé ou None si MinIO est indisponible (non bloquant).
    """
    try:
        client = _client()
        _ensure_bucket(client, BUCKET_PROCESSED)

        key     = f"{name}.csv"
        content = df.to_csv(index=False).encode("utf-8")
        buf     = io.BytesIO(content)

        client.put_object(
            BUCKET_PROCESSED, key, buf, length=len(content), content_type="text/csv"
        )
        logger.info("Data Lake MinIO processed -> %s/%s", BUCKET_PROCESSED, key)
        return key
    except Exception as exc:
        logger.debug("Data Lake MinIO indisponible [processed] : %s", exc)
        return None


# ── Lecture ────────────────────────────────────────────────────────────────────

def _filename_to_dt(key: str) -> datetime | None:
    """Extrait le datetime UTC depuis un nom de fichier horodaté."""
    m = re.search(r"(\d{8}_\d{6})", key)
    if not m:
        return None
    try:
        return datetime.strptime(m.group(1), "%Y%m%d_%H%M%S").replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def load_latest_raw(source: str):
    """Charge le dernier snapshot d'une source depuis MinIO."""
    client = _client()
    try:
        objects = sorted(
            client.list_objects(BUCKET_RAW, prefix=f"{source}/"),
            key=lambda o: o.object_name,
        )
    except S3Error:
        return None

    if not objects:
        return None

    response = client.get_object(BUCKET_RAW, objects[-1].object_name)
    try:
        return json.loads(response.read().decode("utf-8"))
    finally:
        response.close()
        response.release_conn()


def load_weather_history(hours: int = 24) -> pd.DataFrame:
    """Construit un historique météo depuis les objets MinIO."""
    client = _client()
    rows = []
    try:
        objects = client.list_objects(BUCKET_RAW, prefix="weather/")
    except S3Error:
        return pd.DataFrame()

    cutoff = datetime.now(timezone.utc).timestamp() - hours * 3600

    for obj in objects:
        dt = _filename_to_dt(obj.object_name)
        if dt is None or dt.timestamp() < cutoff:
            continue
        response = client.get_object(BUCKET_RAW, obj.object_name)
        try:
            w = json.loads(response.read().decode("utf-8"))
        finally:
            response.close()
            response.release_conn()
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
    """Construit un historique de disponibilité parkings depuis MinIO."""
    client = _client()
    rows   = []
    cutoff = datetime.now(timezone.utc).timestamp() - hours * 3600

    for source in ["citedia", "star"]:
        try:
            objects = client.list_objects(BUCKET_RAW, prefix=f"{source}/")
        except S3Error:
            continue

        for obj in objects:
            dt = _filename_to_dt(obj.object_name)
            if dt is None or dt.timestamp() < cutoff:
                continue
            response = client.get_object(BUCKET_RAW, obj.object_name)
            try:
                data = json.loads(response.read().decode("utf-8"))
            finally:
                response.close()
                response.release_conn()

            if source == "citedia" and isinstance(data, list):
                for p in data:
                    rows.append({
                        "snapshot_time": dt,
                        "source":        "citedia",
                        "parking_id":    p.get("id"),
                        "name":          p.get("name"),
                        "free":          p.get("free", 0),
                        "max":           p.get("max", 0),
                    })
            elif source == "star" and isinstance(data, dict):
                for p in data.get("realtime", []):
                    rows.append({
                        "snapshot_time": dt,
                        "source":        "star",
                        "parking_id":    p.get("idparc"),
                        "name":          p.get("nom"),
                        "free":          p.get("jrdinfosoliste", 0),
                        "max":           p.get("capacitesoliste", 0),
                    })

    return pd.DataFrame(rows) if rows else pd.DataFrame()


# ── Introspection du Data Lake ─────────────────────────────────────────────────

def list_objects(bucket: str) -> list[dict]:
    """Liste les objets d'un bucket MinIO avec nom, taille et date de modification.

    Lève une exception si MinIO est inaccessible (l'appelant gère l'affichage).
    """
    result = []
    for obj in _client().list_objects(bucket, recursive=True):
        result.append({
            "key":        obj.object_name,
            "taille":     f"{obj.size / 1024:.1f} Ko" if obj.size else "—",
            "modifié le": obj.last_modified.strftime("%Y-%m-%d %H:%M")
                          if obj.last_modified else "—",
        })
    return result


def load_raw_preview(key: str, bucket: str | None = None) -> list | dict:
    """Télécharge et parse un fichier JSON brut depuis MinIO.

    Lève une exception si MinIO est inaccessible (l'appelant gère l'affichage).
    """
    bucket   = bucket or BUCKET_RAW
    response = _client().get_object(bucket, key)
    try:
        return json.loads(response.read().decode("utf-8"))
    finally:
        response.close()
        response.release_conn()
