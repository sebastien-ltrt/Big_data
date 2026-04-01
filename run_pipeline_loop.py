"""
Lance le pipeline en boucle à intervalle régulier.
Usage : python run_pipeline_loop.py [intervalle_secondes]
Défaut : 300 secondes (5 minutes)
"""
import sys
import time
import logging
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("loop")

INTERVAL = int(sys.argv[1]) if len(sys.argv) > 1 else 300


def main():
    logger.info("Pipeline en boucle — intervalle : %ds. Ctrl+C pour arrêter.", INTERVAL)
    while True:
        try:
            from src.pipeline import run
            run()
        except Exception as exc:
            logger.error("Erreur pipeline : %s", exc)
        logger.info("Prochain run dans %ds...", INTERVAL)
        time.sleep(INTERVAL)


if __name__ == "__main__":
    main()
