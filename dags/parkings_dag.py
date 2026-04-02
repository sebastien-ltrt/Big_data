"""
DAG Airflow — Pipeline Parkings Rennes
Planifié toutes les 15 minutes.

Graphe :
    fetch_citedia ──┐
    fetch_star    ──┼──► transform ──► load_warehouse
    scrape_weather ─┘               └──► load_weather
"""
import sys
from datetime import datetime, timedelta
from airflow.decorators import dag, task

sys.path.insert(0, "/opt/airflow")

DEFAULT_ARGS = {
    "owner": "parkings",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": False,
}


@dag(
    dag_id="parkings_rennes_pipeline",
    description="Ingestion, transformation et chargement des données Parkings de Rennes",
    schedule_interval="*/15 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["parkings", "rennes", "big-data"],
)
def parkings_pipeline():

    @task(task_id="fetch_citedia")
    def fetch_citedia() -> list:
        from src.controllers.ingestion.citedia import run_ingestion_citedia
        return run_ingestion_citedia()

    @task(task_id="fetch_star")
    def fetch_star() -> dict:
        from src.controllers.ingestion.star import run_ingestion_star
        return run_ingestion_star()

    @task(task_id="scrape_weather")
    def scrape_weather() -> dict:
        from src.controllers.ingestion.weather import run_scraping
        return run_scraping()

    @task(task_id="transform")
    def transform(citedia: list, star: dict, weather: dict) -> list:
        from src.controllers.transform import run_transform
        from src.models.data_lake import save_processed
        df = run_transform(citedia, star, weather)
        save_processed(df, name="latest")
        return df.to_dict("records")

    @task(task_id="load_warehouse")
    def load_warehouse(records: list) -> None:
        import pandas as pd
        from src.models.warehouse import upsert_parkings, insert_availability
        df = pd.DataFrame(records)
        upsert_parkings(df)
        insert_availability(df)

    @task(task_id="load_weather")
    def load_weather(weather: dict) -> None:
        from src.models.warehouse import insert_weather
        insert_weather(weather)

    # Flux
    citedia = fetch_citedia()
    star    = fetch_star()
    weather = scrape_weather()
    records = transform(citedia, star, weather)
    load_warehouse(records)
    load_weather(weather)


parkings_pipeline()
