"""
Pipeline principal — Parkings Rennes
Ingestion API Citedia + STAR → Scraping météo → Transform → Data Lake → PostgreSQL
"""
import logging
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


def setup_logging():
    Path("logs").mkdir(exist_ok=True)
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s — %(message)s")
    handlers = [
        RotatingFileHandler("logs/pipeline.log", maxBytes=5*1024*1024, backupCount=3),
        logging.StreamHandler(sys.stdout),
    ]
    for h in handlers:
        h.setFormatter(fmt)
    logging.basicConfig(level=logging.INFO, handlers=handlers)


def run():
    setup_logging()
    logger = logging.getLogger("pipeline")
    logger.info("=" * 60)
    logger.info("Démarrage du pipeline Parkings Rennes")

    # 1. Ingestion API Citedia
    logger.info("Étape 1/5 — Ingestion API Citedia (parkings centre-ville)")
    from src.ingestion.fetch_citedia import run_ingestion_citedia
    citedia = run_ingestion_citedia()

    # 2. Ingestion API STAR
    logger.info("Étape 2/5 — Ingestion API STAR (parcs-relais P+R)")
    from src.ingestion.fetch_star import run_ingestion_star
    star = run_ingestion_star()

    # 3. Scraping météo
    logger.info("Étape 3/5 — Scraping météo (wttr.in)")
    from src.ingestion.scrape_weather import run_scraping
    weather = run_scraping()

    # 4. Transformation
    logger.info("Étape 4/5 — Transformation et enrichissement")
    from src.processing.transform import run_transform
    from src.storage.data_lake import save_processed
    df = run_transform(citedia, star, weather)
    save_processed(df, name="latest")

    # 5. Chargement PostgreSQL
    logger.info("Étape 5/5 — Chargement PostgreSQL")
    try:
        from src.storage.warehouse import upsert_parkings, insert_availability, insert_weather
        upsert_parkings(df)
        insert_availability(df)
        insert_weather(weather)
        logger.info("PostgreSQL — chargement OK")
    except Exception as exc:
        logger.warning("PostgreSQL indisponible (%s) — données en CSV uniquement", exc)

    logger.info(
        "Pipeline terminé — %d parkings | %d places libres | %d critiques | %s°C",
        len(df), int(df["free_spaces"].sum()),
        int(df["is_critical"].sum()),
        weather.get("temperature_c", "N/A"),
    )
    logger.info("=" * 60)


if __name__ == "__main__":
    run()
