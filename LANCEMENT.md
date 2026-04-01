# Commandes de lancement — Parkings Rennes

## Mode 1 — Sans Docker (local)

### Prérequis (une seule fois)
```bash
# Activer l'environnement virtuel
cd ~/Documents/projets/big_data
source venv/bin/activate

# Installer les dépendances
pip install -r requirements.txt
```

### Lancer le pipeline une fois
```bash
source venv/bin/activate
python src/pipeline.py
```

### Lancer le pipeline en boucle (toutes les 5 min)
```bash
# Terminal 1
source venv/bin/activate
python run_pipeline_loop.py 300
```

### Lancer le dashboard
```bash
# Terminal 2
source venv/bin/activate
streamlit run src/dashboard/app.py
# → Ouvrir http://localhost:8501
```

---

## Mode 2 — Avec Docker (stack complet)

### Prérequis (une seule fois)
```bash
# Autoriser Docker sans sudo (redémarrer la session après)
sudo usermod -aG docker $USER
newgrp docker
```

### Démarrer tout le stack
```bash
cd ~/Documents/projets/big_data
newgrp docker
docker compose up --build -d
```

### Vérifier que tout tourne
```bash
docker compose ps
```

### Voir les logs Airflow
```bash
docker compose logs airflow-init
docker compose logs airflow-scheduler
```

### Arrêter le stack
```bash
docker compose down
```

### Arrêter et supprimer les volumes (reset complet)
```bash
docker compose down -v --remove-orphans
```

### Accès
| Service | URL | Identifiants |
|---|---|---|
| Dashboard Streamlit | http://localhost:8501 | — |
| Airflow UI | http://localhost:8081 | admin / admin |
| PostgreSQL | localhost:5434 | parking_user / parking2024 |

---

## PostgreSQL — Commandes utiles

### Se connecter
```bash
psql -h localhost -U parking_user -d parkings_rennes
# Mot de passe : parking2024
```

### Voir les données
```sql
-- Nombre de snapshots enregistrés
SELECT COUNT(*) FROM availability_snapshots;

-- Dernière disponibilité par parking
SELECT p.name, a.free_spaces, a.occupancy_rate, a.snapshot_time
FROM parkings p
JOIN LATERAL (
    SELECT * FROM availability_snapshots
    WHERE parking_id = p.parking_id
    ORDER BY snapshot_time DESC LIMIT 1
) a ON TRUE
ORDER BY a.occupancy_rate DESC;

-- Historique météo
SELECT * FROM weather_snapshots ORDER BY scraped_at DESC LIMIT 10;
```

---

## Résolution des problèmes courants

### "No module named 'src'"
```bash
# Toujours lancer depuis la racine du projet
cd ~/Documents/projets/big_data
source venv/bin/activate
python src/pipeline.py
```

### "Permission denied" Docker
```bash
newgrp docker
# puis relancer la commande docker
```

### Port déjà utilisé (Docker)
```bash
docker compose down --remove-orphans
docker compose up -d
```

### PostgreSQL — authentification échouée
```bash
sudo sed -i 's/ident/md5/g' /var/lib/pgsql/data/pg_hba.conf
sudo systemctl restart postgresql
```
