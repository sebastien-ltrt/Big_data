@echo off
:: ============================================================
:: start.bat — Lance le pipeline en boucle + le dashboard
:: Usage : double-clic ou start.bat dans un terminal
:: ============================================================

set PROJECT_DIR=%~dp0
cd /d "%PROJECT_DIR%"

echo ============================================
echo   Parkings Rennes - Demarrage
echo ============================================

:: Vérifie que le venv existe
if not exist "venv\Scripts\activate.bat" (
    echo [INFO] Creation de l'environnement virtuel...
    python -m venv venv
    call venv\Scripts\activate.bat
    pip install -r requirements.txt -q
    echo [INFO] Dependances installees.
) else (
    call venv\Scripts\activate.bat
)

:: Premier run immédiat du pipeline
echo.
echo [1/2] Lancement du pipeline initial...
python src\pipeline.py

:: Lance le pipeline en boucle dans une nouvelle fenêtre
echo.
echo [2/2] Pipeline en boucle (toutes les 5 min) en arriere-plan...
start "Pipeline Loop" /min cmd /c "venv\Scripts\activate.bat && python run_pipeline_loop.py 300 > logs\pipeline_loop.log 2>&1"

:: Lance le dashboard
echo.
echo [INFO] Demarrage du dashboard → http://localhost:8501
echo [INFO] Fermez cette fenetre pour tout arreter.
echo ============================================
streamlit run src\dashboard\app.py --server.port 8501

pause
