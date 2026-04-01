#!/usr/bin/env bash
# ============================================================
# start.sh — Lance le pipeline en boucle + le dashboard
# Usage : bash start.sh
# ============================================================

set -e

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$PROJECT_DIR"

VENV="$PROJECT_DIR/venv/bin/activate"

echo "============================================"
echo "  Parkings Rennes — Démarrage"
echo "============================================"

# Vérifie que le venv existe
if [ ! -f "$VENV" ]; then
    echo "[INFO] Création de l'environnement virtuel..."
    python3 -m venv venv
    source "$VENV"
    pip install -r requirements.txt -q
    echo "[INFO] Dépendances installées."
else
    source "$VENV"
fi

# Premier run immédiat du pipeline
echo ""
echo "[1/2] Lancement du pipeline initial..."
python src/pipeline.py

# Lance le pipeline en boucle en arrière-plan (toutes les 5 min)
echo ""
echo "[2/2] Pipeline en boucle (toutes les 5 min) en arrière-plan..."
python run_pipeline_loop.py 300 > logs/pipeline_loop.log 2>&1 &
LOOP_PID=$!
echo "[INFO] Pipeline loop PID : $LOOP_PID"

# Lance le dashboard (bloquant — reste ouvert)
echo ""
echo "[INFO] Démarrage du dashboard → http://localhost:8501"
echo "[INFO] Ctrl+C pour tout arrêter."
echo "============================================"

# Quand on fait Ctrl+C, on arrête aussi le pipeline loop
trap "echo ''; echo '[INFO] Arrêt...'; kill $LOOP_PID 2>/dev/null; exit 0" INT TERM

streamlit run src/dashboard/app.py \
    --server.port 8501 \
    --server.headless false
