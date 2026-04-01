# Plan — Pipeline Big Data Parkings Rennes

## Contexte

Le projet consiste à concevoir un pipeline de données complet sur les parkings intelligents de Rennes, couvrant l'ingestion, le stockage brut, la transformation, le chargement en base et la visualisation. L'objectif est d'avoir un système fonctionnel et démontrable en soutenance.

La quasi-totalité du code est déjà implémentée et fonctionnelle. Ce plan détaille l'état actuel et ce qu'il reste à finaliser pour une soutenance réussie.

---

## État actuel — Ce qui fonctionne déjà ✅

| Composant | Fichier | Statut |
|---|---|---|
| Ingestion API Citedia (10 parkings centre-ville) | `src/ingestion/fetch_citedia.py` | ✅ Fonctionnel |
| Ingestion API STAR (8 parcs-relais P+R) | `src/ingestion/fetch_star.py` | ✅ Fonctionnel |
| Scraping météo wttr.in (BeautifulSoup) | `src/ingestion/scrape_weather.py` | ✅ Fonctionnel |
| Data Lake local (JSON horodatés) | `src/storage/data_lake.py` | ✅ Fonctionnel |
| Transformation pandas (fusion + KPIs) | `src/processing/transform.py` | ✅ Fonctionnel |
| Data Warehouse PostgreSQL | `src/storage/warehouse.py` + `sql/create_tables.sql` | ✅ Implémenté |
| Pipeline orchestrateur | `src/pipeline.py` | ✅ Fonctionnel |
| DAG Airflow (toutes les 15 min) | `dags/parkings_dag.py` | ✅ Implémenté |
| Dashboard Streamlit | `src/dashboard/app.py` | ✅ Fonctionnel |
| Docker Compose (stack complet) | `docker-compose.yml` | ✅ Fonctionnel |

---

## Ce qu'il reste à faire — Étapes prioritaires

### Étape 1 — Connecter PostgreSQL en local (15 min)
**Problème :** Le pipeline fonctionne mais PostgreSQL local rejette l'authentification (`parking_user`).
**Solution :**
```bash
sudo sed -i 's/ident/md5/g' /var/lib/pgsql/data/pg_hba.conf
sudo systemctl restart postgresql
sudo -u postgres psql -c "CREATE DATABASE parkings_rennes;"
sudo -u postgres psql -c "CREATE USER parking_user WITH PASSWORD 'parking2024';"
sudo -u postgres psql -c "GRANT ALL PRIVILEGES ON DATABASE parkings_rennes TO parking_user;"
sudo -u postgres psql -d parkings_rennes -f sql/create_tables.sql
```
**Résultat attendu :** `python src/pipeline.py` charge les données dans PostgreSQL.

---

### Étape 2 — Accumuler de l'historique (1h minimum)
**Pourquoi :** Les onglets "Tendances 24h" et "Météo" nécessitent plusieurs snapshots pour afficher des courbes.
**Solution :** Lancer le pipeline en boucle pendant au moins 1h avant la soutenance.
```bash
# Terminal 1 — pipeline toutes les 5 minutes
source venv/bin/activate
python run_pipeline_loop.py 300   # 300 secondes = 5 min

# Terminal 2 — dashboard
streamlit run src/dashboard/app.py
```
**Alternative avec Docker :** Airflow se charge de l'automatisation toutes les 15 min.

---

### Étape 3 — Vérifier le stack Docker complet (30 min)
**Problème résolu :** Ports changés (8081 pour Airflow, 5434 pour PostgreSQL).
**Vérification :**
```bash
docker compose ps          # Tous les conteneurs doivent être "healthy"
docker compose logs airflow-init   # Doit se terminer avec "Admin user created"
```
**Accès :**
- Airflow UI → http://localhost:8081 (admin/admin)
- Dashboard → http://localhost:8501

---

### Étape 4 — Mettre à jour le README.md (20 min)
**Fichier :** `README.md` (actuellement vide)
**Contenu à ajouter :**
- Description du projet (parkings Rennes, sources de données)
- Architecture (schéma du pipeline)
- Instructions d'installation et de lancement
- Captures d'écran du dashboard

---

### Étape 5 — Préparer la présentation soutenance

#### Structure recommandée (10-15 min de présentation)
1. **Problématique** (1 min) — Pourquoi les parkings intelligents ? Enjeux de la mobilité urbaine à Rennes
2. **Architecture** (2 min) — Schéma du pipeline complet (ingestion → data lake → transform → warehouse → dashboard)
3. **Sources de données** (2 min) — API Citedia (10 parkings CV), API STAR (8 P+R), scraping météo wttr.in
4. **Démonstration live** (5 min) — Lancer `python src/pipeline.py` devant le jury, montrer les logs, ouvrir le dashboard
5. **Airflow** (2 min) — Montrer le DAG dans l'UI, expliquer l'orchestration automatique
6. **Données PostgreSQL** (1 min) — Montrer les 3 tables avec quelques requêtes SQL
7. **Bilan et perspectives** (2 min) — Ce qui pourrait être amélioré (alerting, prédiction, API REST)

#### Points forts à mettre en avant
- **2 APIs différentes** fusionnées dans un schéma unifié
- **Scraping BeautifulSoup** en complément des APIs
- **Architecture en couches** : Data Lake (brut) → PostgreSQL (structuré) → Dashboard (visualisé)
- **Airflow** pour l'orchestration automatique (stack utilisé en production par Airbnb, Uber)
- **Docker Compose** pour la reproductibilité totale
- **18 parkings** en temps réel avec données spécialisées (EV, covoiturage, PMR)

---

## Workflow complet du projet (rappel)

```
Internet
  │
  ├── API Citedia ──────────────────┐
  │   (10 parkings centre-ville)    │
  │                                 ▼
  ├── API STAR ──────────────────► fetch → Data Lake (data/raw/)
  │   (8 parcs-relais P+R)          │          JSON horodatés
  │                                 │
  └── Scraping wttr.in ────────────┘
      (BeautifulSoup)               │
                                    ▼
                              transform.py
                              (pandas fusion)
                                    │
                         ┌──────────┴──────────┐
                         ▼                     ▼
                    PostgreSQL            data/processed/
                    (3 tables)            latest.csv
                         │                     │
                         └──────────┬──────────┘
                                    ▼
                             Streamlit Dashboard
                             localhost:8501
                                    ▲
                             Airflow DAG
                             (toutes les 15 min)
```

---

## Fichiers critiques à connaître pour la soutenance

| Fichier | Rôle | À montrer |
|---|---|---|
| `src/pipeline.py` | Point d'entrée du pipeline | Oui — lancer live |
| `src/ingestion/fetch_citedia.py` | API Citedia | Oui — montrer les requêtes HTTP |
| `src/ingestion/scrape_weather.py` | BeautifulSoup scraping | Oui — expliquer le parsing HTML |
| `src/processing/transform.py` | Fusion pandas | Oui — montrer `run_transform()` |
| `src/storage/data_lake.py` | Stockage brut | Oui — montrer les fichiers JSON |
| `src/storage/warehouse.py` | PostgreSQL | Oui — montrer les tables |
| `dags/parkings_dag.py` | DAG Airflow | Oui — montrer le graphe dans l'UI |
| `src/dashboard/app.py` | Dashboard Streamlit | Oui — démo live |
| `docker-compose.yml` | Stack Docker | Oui — montrer `docker compose ps` |
| `sql/create_tables.sql` | Schéma DB | Optionnel |

---

## Vérification finale avant soutenance

```bash
# 1. Pipeline fonctionne
source venv/bin/activate
python src/pipeline.py
# → Doit afficher "Pipeline terminé — 18 parkings | X places libres"

# 2. Dashboard s'ouvre
streamlit run src/dashboard/app.py
# → http://localhost:8501 doit montrer la carte avec 18 parkings

# 3. Docker stack opérationnel
docker compose ps
# → Tous les services doivent être "healthy" ou "running"

# 4. Airflow UI accessible
# → http://localhost:8081, DAG "parkings_rennes_pipeline" visible et actif

# 5. PostgreSQL contient des données
psql -h localhost -p 5434 -U parking_user -d parkings_rennes \
  -c "SELECT COUNT(*) FROM availability_snapshots;"
# → Doit retourner > 0
```
