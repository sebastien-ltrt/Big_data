#!/usr/bin/env bash
# ============================================================
# start.sh — Lance tout le stack (Docker + pipeline + dashboard)
# Usage : bash start.sh
# ============================================================

set -e
PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$PROJECT_DIR"

echo "============================================"
echo "  Parkings Rennes — Démarrage complet"
echo "============================================"

# ── 1. Docker Compose ────────────────────────────────────────
echo ""
echo "[1/4] Démarrage des services Docker (MinIO, PostgreSQL, Airflow)..."
newgrp docker 2>/dev/null || true
docker compose up --build -d

echo "[INFO] Attente que les services soient prêts..."
docker compose wait postgres-parkings 2>/dev/null || sleep 15

# ── 2. Environnement Python ──────────────────────────────────
echo ""
echo "[2/4] Préparation de l'environnement Python..."
VENV="$PROJECT_DIR/venv/bin/activate"
if [ ! -f "$VENV" ]; then
    echo "[INFO] Création du venv..."
    python3 -m venv venv
    source "$VENV"
    pip install -r requirements.txt -q
else
    source "$VENV"
fi

# ── 3. Premier run du pipeline ───────────────────────────────
echo ""
echo "[3/4] Lancement du pipeline initial..."
python src/pipeline.py

# ── 4. Pipeline en boucle en arrière-plan ───────────────────
echo ""
echo "[4/4] Pipeline en boucle (toutes les 5 min) en arrière-plan..."
python run_pipeline_loop.py 300 > logs/pipeline_loop.log 2>&1 &
LOOP_PID=$!
echo "[INFO] Pipeline loop PID : $LOOP_PID"

# ── Accès ────────────────────────────────────────────────────
echo ""
echo "============================================"
echo "  Tout est lancé !"
echo "  Dashboard   → http://localhost:8501"
echo "  Airflow UI  → http://localhost:8081  (admin/admin)"
echo "  MinIO       → http://localhost:9001  (minioadmin/minioadmin)"
echo "  Ctrl+C pour tout arrêter."
echo "============================================"

trap "echo ''; echo '[INFO] Arrêt...'; kill $LOOP_PID 2>/dev/null; docker compose down; exit 0" INT TERM

# Garde le script actif (les services tournent en Docker)
wait $LOOP_PID
