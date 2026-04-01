# Parkings Rennes — Pipeline Big Data

Pipeline de données complet sur la disponibilité en temps réel des parkings intelligents de Rennes.

## Sources de données

| Source | Type | Données |
|---|---|---|
| [API Citedia](http://data.citedia.com/r1/parks) | API REST | 10 parkings centre-ville — places libres, statut |
| [API STAR](https://data.explore.star.fr) | API REST | 8 parcs-relais P+R — places VE, covoiturage, PMR |
| [wttr.in/Rennes](https://wttr.in/Rennes) | Scraping HTML | Météo Rennes — température, humidité, vent |

## Architecture

```
API Citedia ──┐
API STAR    ──┼──► Data Lake (data/raw/)  ──► Transform (pandas) ──► PostgreSQL
wttr.in     ──┘    JSON horodatés                                └──► CSV fallback
                                                                       │
                                                                       ▼
                                                               Dashboard Streamlit
                                                               ↑
                                                         Airflow DAG (15 min)
```

## Technologies

- **Ingestion** : Python `requests` + `BeautifulSoup`
- **Data Lake** : Fichiers JSON horodatés locaux (`data/raw/`)
- **Transformation** : `pandas`
- **Data Warehouse** : PostgreSQL + `psycopg2`
- **Orchestration** : Apache Airflow (DAG toutes les 15 min)
- **Dashboard** : Streamlit + Plotly
- **Infrastructure** : Docker Compose

## Lancement rapide

```bash
# 1. Environnement virtuel
python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt

# 2. Une seule exécution du pipeline
python src/pipeline.py

# 3. Dashboard
streamlit run src/dashboard/app.py
# → http://localhost:8501

# 4. Pipeline en boucle (toutes les 5 min)
python run_pipeline_loop.py 300
```

## Avec Docker (stack complet)

```bash
newgrp docker
docker compose up --build -d

# Airflow UI → http://localhost:8081  (admin / admin)
# Dashboard  → http://localhost:8501
```

## Structure du projet

```
├── src/
│   ├── pipeline.py              # Orchestrateur principal
│   ├── ingestion/
│   │   ├── fetch_citedia.py     # API Citedia (10 parkings centre-ville)
│   │   ├── fetch_star.py        # API STAR (8 parcs-relais P+R)
│   │   └── scrape_weather.py    # Scraping météo wttr.in
│   ├── processing/
│   │   └── transform.py         # Fusion + nettoyage + KPIs
│   ├── storage/
│   │   ├── data_lake.py         # Lecture/écriture JSON horodatés
│   │   └── warehouse.py         # PostgreSQL (upsert/insert/select)
│   └── dashboard/
│       └── app.py               # Streamlit (carte, tendances, météo)
├── dags/
│   └── parkings_dag.py          # DAG Airflow
├── sql/
│   └── create_tables.sql        # Schéma PostgreSQL
├── docker-compose.yml           # Stack complet
├── Dockerfile                   # Image Airflow
└── Dockerfile.streamlit         # Image Streamlit
```

## Dashboard

5 onglets disponibles :

| Onglet | Contenu |
|---|---|
| 🗺️ Carte | Carte interactive des 18 parkings, colorés selon le taux d'occupation |
| 🏙️ Centre-ville | Jauges et bar chart des 10 parkings Citedia |
| 🚌 Parcs-Relais | Disponibilité P+R avec places EV, covoiturage, PMR |
| 📈 Tendances 24h | Courbes historiques + heatmap par parking |
| 🌤️ Météo | Historique température, humidité, vent |
