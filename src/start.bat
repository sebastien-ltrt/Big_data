@echo off
:: ============================================================
:: start.bat — Lance tout le stack (Docker + pipeline + dashboard)
:: Usage : double-clic ou start.bat dans un terminal
:: ============================================================

set PROJECT_DIR=%~dp0
cd /d "%PROJECT_DIR%"

echo ============================================
echo   Parkings Rennes - Demarrage complet
echo ============================================

:: ── 0. Fichier .env ────────────────────────────────────────
if not exist ".env" (
    echo.
    echo [INFO] .env absent - copie depuis .env.example...
    copy ".env.example" ".env" >nul
    echo [INFO] .env cree. Valeurs par defaut utilisees (compatibles Docker).
)

:: ── 1. Docker Compose ──────────────────────────────────────
echo.
echo [1/4] Demarrage des services Docker (MinIO, PostgreSQL, Airflow)...
docker compose up --build -d
if errorlevel 1 (
    echo [ERREUR] Docker compose a echoue. Docker Desktop est-il lance ?
    pause
    exit /b 1
)

echo [INFO] Attente que les services soient prets (20s)...
timeout /t 20 /nobreak >nul

:: ── 2. Environnement Python ────────────────────────────────
echo.
echo [2/4] Preparation de l'environnement Python...
if not exist "venv\Scripts\activate.bat" (
    echo [INFO] Creation du venv...
    python -m venv venv
    call venv\Scripts\activate.bat
    pip install -r requirements.txt -q
) else (
    call venv\Scripts\activate.bat
)

:: ── 3. Premier run du pipeline ─────────────────────────────
echo.
echo [3/4] Lancement du pipeline initial...
python -m src.controllers.pipeline
if errorlevel 1 (
    echo [AVERTISSEMENT] Le pipeline a rencontre une erreur. Verification des logs...
)

:: ── 4. Pipeline en boucle en arriere-plan ──────────────────
echo.
echo [4/4] Pipeline en boucle (toutes les 5 min) en arriere-plan...
if not exist "logs" mkdir logs
start "Pipeline Loop" /min cmd /c "call venv\Scripts\activate.bat && python run_pipeline_loop.py 300 >> logs\pipeline_loop.log 2>&1"

:: ── Acces ───────────────────────────────────────────────────
echo.
echo ============================================
echo   Tout est lance !
echo.
echo   Dashboard   -^> http://localhost:8501
echo   Airflow UI  -^> http://localhost:8081  (admin / admin)
echo   MinIO       -^> http://localhost:9001  (minioadmin / minioadmin)
echo   PostgreSQL  -^> localhost:5434         (parking_user / parking2024)
echo.
echo   Fermez cette fenetre pour arreter le pipeline en boucle.
echo   Tapez : docker compose down   pour arreter Docker.
echo ============================================
echo.

:: Ouvre le dashboard dans le navigateur
start "" http://localhost:8501

pause

:: Arret propre
echo [INFO] Arret des services Docker...
docker compose down
