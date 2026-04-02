"""
Transformation — fusion, nettoyage et calcul des KPIs parkings.
Combine Citedia (centre-ville) + STAR P+R + météo.
"""
import logging
from datetime import datetime, timezone

import pandas as pd

logger = logging.getLogger(__name__)


# ── Citedia ───────────────────────────────────────────────────────────────────

def normalize_citedia(parks: list[dict]) -> pd.DataFrame:
    """Normalise les données Citedia (10 parkings centre-ville)."""
    # Coordonnées GPS connues des parkings Citedia (fixes)
    COORDS = {
        "colombier":        (48.1054, -1.6793),
        "gare-sud":         (48.1034, -1.6716),
        "dinan-chezy":      (48.1175, -1.6889),
        "hoche":            (48.1116, -1.6748),
        "kennedy":          (48.1090, -1.6959),
        "lices":            (48.1127, -1.6736),
        "charles-de-gaulle":(48.1033, -1.6732),
        "hotel-dieu":       (48.1158, -1.6819),
        "kleber":           (48.1072, -1.6756),
        "arsenal":          (48.1088, -1.6733),
    }
    rows = []
    for p in parks:
        pid = p.get("id", "")
        lat, lon = COORDS.get(pid, (None, None))
        total = p.get("max", 0) or 0
        free  = p.get("free", 0) or 0
        occupied = total - free
        rows.append({
            "parking_id":       pid,
            "name":             p.get("name"),
            "source":           "citedia",
            "type":             "Centre-ville",
            "lat":              lat,
            "lon":              lon,
            "total_spaces":     total,
            "free_spaces":      free,
            "occupied_spaces":  occupied,
            "status":           p.get("status"),
            "is_open":          p.get("status") != "CLOSED",
            "snapshot_time":    datetime.now(timezone.utc),
        })
    df = pd.DataFrame(rows)
    logger.info("Citedia — %d parkings normalisés", len(df))
    return df


# ── STAR P+R ──────────────────────────────────────────────────────────────────

def normalize_star_realtime(records: list[dict]) -> pd.DataFrame:
    """Normalise les parcs-relais STAR temps réel."""
    rows = []
    for r in records:
        coords = r.get("coordonnees") or {}
        total  = r.get("capacitesoliste", 0) or 0
        free   = r.get("jrdinfosoliste", 0) or 0
        rows.append({
            "parking_id":          r.get("idparc"),
            "name":                r.get("nom"),
            "source":              "star",
            "type":                "Parc-Relais",
            "lat":                 coords.get("lat"),
            "lon":                 coords.get("lon"),
            "total_spaces":        total,
            "free_spaces":         free,
            "occupied_spaces":     total - free,
            "status":              r.get("etatremplissage"),
            "is_open":             r.get("etatouverture") == "OUVERT",
            # Spécifique P+R
            "free_ev":             r.get("jrdinfoelectrique", 0),
            "total_ev":            r.get("capaciteve", 0),
            "free_carpool":        r.get("jrdinfocovoiturage", 0),
            "total_carpool":       r.get("capacitecovoiturage", 0),
            "free_pmr":            r.get("jrdinfopmr", 0),
            "total_pmr":           r.get("capacitepmr", 0),
            "elevators_total":     r.get("nbascenseur", 0),
            "elevators_available": r.get("nbascenseurdispo"),
            "snapshot_time":       datetime.now(timezone.utc),
        })
    df = pd.DataFrame(rows)
    logger.info("STAR P+R — %d parcs normalisés", len(df))
    return df


def normalize_star_topology(records: list[dict]) -> pd.DataFrame:
    """Normalise la topologie STAR (adresses)."""
    rows = []
    for r in records:
        rows.append({
            "parking_id": r.get("idparc"),
            "address":    r.get("adresse") or r.get("adressevoie", ""),
            "city":       r.get("commune", "Rennes"),
        })
    return pd.DataFrame(rows).dropna(subset=["parking_id"])


# ── Fusion & enrichissement ───────────────────────────────────────────────────

def add_weather(df: pd.DataFrame, weather: dict) -> pd.DataFrame:
    """Ajoute le contexte météo à chaque ligne."""
    df = df.copy()
    df["temperature_c"]       = weather.get("temperature_c")
    df["humidity_pct"]        = weather.get("humidity_pct")
    df["wind_speed_kmh"]      = weather.get("wind_speed_kmh")
    df["weather_description"] = weather.get("weather_description")
    return df


def compute_kpis(df: pd.DataFrame) -> pd.DataFrame:
    """Calcule taux d'occupation et flag critique."""
    df = df.copy()
    df["occupancy_rate"] = df.apply(
        lambda r: round((r["occupied_spaces"] / r["total_spaces"]) * 100, 1)
        if r["total_spaces"] > 0 else 0.0, axis=1
    )
    df["is_critical"] = (df["free_spaces"] <= 20) & df["is_open"]
    df["is_full"]     = (df["free_spaces"] == 0)  & df["is_open"]
    return df


def run_transform(citedia: list[dict], star: dict, weather: dict) -> pd.DataFrame:
    """Orchestre toute la transformation. Retourne le DataFrame final unifié."""
    df_citedia = normalize_citedia(citedia)

    df_star = normalize_star_realtime(star["realtime"])
    df_topo = normalize_star_topology(star["topology"])
    df_star = df_star.merge(df_topo, on="parking_id", how="left")

    # Colonnes communes seulement pour Citedia (pas de EV/covoiturage)
    for col in ["free_ev", "total_ev", "free_carpool", "total_carpool",
                "free_pmr", "total_pmr", "elevators_total", "elevators_available",
                "address", "city"]:
        if col not in df_citedia.columns:
            df_citedia[col] = None

    df = pd.concat([df_citedia, df_star], ignore_index=True)
    df = add_weather(df, weather)
    df = compute_kpis(df)

    logger.info(
        "Transform OK — %d parkings | %d places libres | %d critiques | %s°C",
        len(df), int(df["free_spaces"].sum()),
        int(df["is_critical"].sum()),
        weather.get("temperature_c", "N/A"),
    )
    return df
