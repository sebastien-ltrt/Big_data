# 🅿️ Parking Rennes — Pipeline Big Data

![Python](https://img.shields.io/badge/Python-3.11-blue?logo=python&logoColor=white)
![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.x-017CEE?logo=apacheairflow&logoColor=white)
![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.x-E25A1C?logo=apachespark&logoColor=white)
![MinIO](https://img.shields.io/badge/MinIO-Data%20Lake-C72E49?logo=minio&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-336791?logo=postgresql&logoColor=white)
![Streamlit](https://img.shields.io/badge/Streamlit-Dashboard-FF4B4B?logo=streamlit&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white)

Pipeline Big Data complet pour visualiser la disponibilité en temps réel des parkings intelligents de Rennes. Le projet couvre toute la chaîne ETL : ingestion multi-sources, stockage dans un Data Lake S3-compatible, transformation Pandas/Spark, chargement dans un Data Warehouse PostgreSQL, et visualisation via un dashboard Streamlit interactif.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        INGESTION (toutes les 15 min)                │
│                                                                     │
│   API Citedia ──┐                                                   │
│   (10 parkings) │                                                   │
│                 ├──► Airflow DAG ──► Data Lake MinIO (JSON bruts)   │
│   API STAR ─────┤                   s3://parkings-raw/              │
│   (8 P+R)       │                          │                        │
│                 │              ┌───────────┘                        │
│   wttr.in  ─────┘              │  Transform (Pandas)                │
│   (météo)                      │  + Spark (agrégations)             │
└───────────────────────────────┼─────────────────────────────────────┘
                                │
                    ┌───────────▼──────────────┐
                    │  Data Warehouse          │
                    │  PostgreSQL              │
                    │  • parkings              │
                    │  • availability_snapshots│
                    │  • weather_snapshots     │
                    └───────────┬──────────────┘
                                │
                    ┌───────────▼──────────────┐
                    │  Dashboard Streamlit      │
                    │  http://localhost:8501    │
                    │  • Carte interactive      │
                    │  • Tendances 24h          │
                    │  • Météo                  │
                    └──────────────────────────┘
```

## Technologies

| Couche | Technologie | Rôle |
|---|---|---|
| Ingestion | Python `requests` + `BeautifulSoup` | Appels API REST + scraping HTML |
| Orchestration | Apache Airflow (DAG `*/15 * * * *`) | Planification et retry automatique |
| Data Lake | MinIO (S3-compatible) | Stockage append-only des snapshots JSON |
| Transformation | Pandas + Apache Spark | Nettoyage, KPIs, agrégations horaires |
| Data Warehouse | PostgreSQL 16 | Historique et requêtes analytiques |
| Dashboard | Streamlit + Plotly | Visualisation temps réel |
| Infrastructure | Docker Compose | Stack complet en un seul fichier |

---

## Sources de données & APIs

| Source | Type | Clé API | Lien | Données |
|---|---|---|---|---|
| **Citedia** | API REST JSON | ❌ Aucune (publique) | http://data.citedia.com/r1/parks | 10 parkings centre-ville — places libres, statut |
| **STAR Open Data** | API REST JSON | ❌ Aucune (publique) | https://data.explore.star.fr/api/explore/v2.1 | 8 parcs-relais P+R — places EV, covoiturage, PMR |
| **wttr.in** | JSON / Scraping HTML | ❌ Aucune (publique) | https://wttr.in/Rennes?format=j1 | Météo Rennes — température, humidité, vent |
| **Open-Meteo** | API REST JSON | ❌ Aucune (publique) | https://api.open-meteo.com/v1/forecast | Météo fallback — température, humidité, vent, code météo |
| **Nominatim (OSM)** | API REST JSON | ❌ Aucune (publique) | https://nominatim.openstreetmap.org/search | Géocodage d'adresses (recherche parking le plus proche) |

> Toutes les APIs utilisées dans ce projet sont **libres d'accès et sans clé API**.

---

## Prérequis

- **Docker Desktop** (ou Docker Engine + Compose v2) — [installer](https://docs.docker.com/get-docker/)
- **Python 3.11+** (pour le mode local sans Docker)
- **8 Go de RAM** recommandés (Airflow + Spark + MinIO + PostgreSQL)

---

## Installation et lancement

### Avec Docker (stack complet recommandé)

```bash
# 1. Cloner le repo
git clone <url-du-repo>
cd parking-rennes-bigdata

# 2. Copier les variables d'environnement
cp .env.example .env
# Éditez .env si nécessaire (les valeurs par défaut fonctionnent avec Docker)

# 3. Lancer tout le stack (un seul script)
bash start.sh
```

Le script `start.sh` démarre Docker Compose, attend que les services soient prêts,
lance un premier run du pipeline, puis boucle toutes les 5 minutes.

**Interfaces disponibles :**

| Service | URL | Identifiants |
|---|---|---|
| Dashboard Streamlit | http://localhost:8501 | — |
| Airflow UI | http://localhost:8081 | admin / admin |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin |
| PostgreSQL | localhost:**5434** | parking_user / parking2024 |

### Ou manuellement (hors Docker)

```bash
# 1. Environnement Python
python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt

# 2. Variables d'environnement (adapter pour localhost)
cp .env.example .env
# Vérifier : PG_HOST=localhost, MINIO_ENDPOINT=localhost:9000

# 3. Démarrer uniquement les services de données
docker compose up minio postgres-parkings -d

# 4. Un run unique du pipeline
python -m src.controllers.pipeline

# 5. Pipeline en boucle (toutes les 5 min)
python run_pipeline_loop.py 300

# 6. Dashboard local
streamlit run src/views/dashboard.py
# → http://localhost:8501
```

---

## Job Spark (agrégations horaires)

Le job Spark lit les snapshots bruts depuis MinIO et produit des agrégations
horaires en Parquet (moyenne mobile sur 3h, min/max de places libres).

```bash
# Avec les JARs S3A (connexion MinIO) :
spark-submit \
  --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.367 \
  spark_jobs/transform_parking.py
```

Résultats écrits dans `s3://parkings-processed/hourly_aggregations/` partitionné par `date` et `source`.

---

## Structure du projet (architecture MVC)

```
.
├── docker-compose.yml              # Stack complet (MinIO, Airflow, PostgreSQL, Streamlit)
├── Dockerfile                      # Image Airflow custom
├── Dockerfile.streamlit            # Image Streamlit
├── start.sh / start.bat            # Script de démarrage one-shot
├── run_pipeline_loop.py            # Boucle pipeline locale (sans Airflow)
├── requirements.txt                # Dépendances Python
├── .env.example                    # Variables d'environnement (template)
│
├── src/
│   │
│   ├── models/                     # ── MODEL : accès aux données ──────────────
│   │   ├── data_lake.py            #   MinIO — save/load JSON + list_objects
│   │   └── warehouse.py            #   PostgreSQL — upsert / insert / select
│   │
│   ├── views/                      # ── VIEW : présentation ────────────────────
│   │   └── dashboard.py            #   Streamlit 6 onglets (carte, tendances…)
│   │
│   └── controllers/                # ── CONTROLLER : logique métier ────────────
│       ├── pipeline.py             #   Orchestrateur ETL principal
│       ├── transform.py            #   Fusion Citedia+STAR, nettoyage, KPIs
│       └── ingestion/
│           ├── citedia.py          #   API Citedia (10 parkings centre-ville)
│           ├── star.py             #   API STAR (8 parcs-relais P+R)
│           └── weather.py          #   Scraping météo wttr.in
│
├── spark_jobs/
│   └── transform_parking.py        # Job Spark — agrégations horaires (Parquet)
│
├── dags/
│   └── parkings_dag.py             # DAG Airflow (toutes les 15 min)
│
├── docker/
│   └── init_db.sql                 # Schéma PostgreSQL (créé au démarrage)
│
└── logs/                           # Logs rotatifs du pipeline local
```

---

## Dashboard Streamlit

6 onglets disponibles :

| Onglet | Contenu |
|---|---|
| 🗺️ Carte | Carte Plotly interactive des 18 parkings, colorée par taux d'occupation (vert / orange / rouge) |
| 🏙️ Centre-ville | Jauges et bar chart horizontal pour les 10 parkings Citedia |
| 🚌 Parcs-Relais | Disponibilité P+R avec détail places EV, covoiturage, PMR |
| 📈 Tendances 24h | Courbes historiques + heatmap taux d'occupation par parking |
| 🌤️ Météo | Évolution température, humidité, vent sur 24h |
| 🗄️ Data Lake | Liste des fichiers MinIO (buckets raw/processed) + prévisualisation JSON |

Le dashboard se rafraîchit automatiquement toutes les 60 secondes.
Si PostgreSQL est inaccessible, il bascule automatiquement sur les données MinIO.

---

## Schéma PostgreSQL

```sql
-- Référentiel des parkings
parkings (parking_id PK, name, source, type, lat, lon, total_spaces, address, city, last_seen)

-- Snapshots de disponibilité (append-only)
availability_snapshots (id, parking_id FK, snapshot_time, free_spaces, occupied_spaces,
                        total_spaces, occupancy_rate, is_open, is_critical, is_full,
                        status, free_ev, free_carpool, free_pmr,
                        temperature_c, humidity_pct, wind_speed_kmh, weather_desc)

-- Historique météo
weather_snapshots (id, scraped_at, temperature_c, humidity_pct,
                   wind_speed_kmh, wind_direction, weather_description, scrape_error)
```

---

## Points d'attention

- **MinIO inaccessible en mode local** — Si Docker n'est pas lancé, les appels `save_raw()` / `save_processed()` échouent silencieusement (log `DEBUG`, pipeline non interrompu). Les données sont alors écrites uniquement dans PostgreSQL et dans `data/processed/latest.csv` (fallback local).
- **Port PostgreSQL : 5434** — `postgres-parkings` est exposé sur le port hôte **5434** (pas 5432) pour éviter les conflits avec un PostgreSQL local. Utilisez `psql -h localhost -p 5434 -U parking_user -d parkings_rennes`.
- **Python 3.11+** — Le venv du projet tourne sur Python 3.14 (dernière version disponible sur la machine). Toute version ≥ 3.11 fonctionne.
- **PySpark non installé par défaut** — Le job `spark_jobs/transform_parking.py` nécessite `pip install pyspark>=3.5` et Java 11+. Voir la section [Job Spark](#job-spark-agrégations-horaires) et `LANCEMENT.md`.
- **`sql/create_tables.sql` supprimé** — La référence unique est `docker/init_db.sql`, monté automatiquement dans le conteneur `postgres-parkings` au premier démarrage.

---

## Contexte scolaire

Projet réalisé dans le cadre du cours **Big Data — B2** à **Sup de Vinci**.

Formateur : **Diallo Alimoú**
