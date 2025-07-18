@echo off
echo Neo4j Knowledge Graph Builder - Windows Setup
echo =============================================

echo Checking Python installation...
python --version >nul 2>&1
if errorlevel 1 (
    echo ERROR: Python is not installed or not in PATH
    echo Please install Python 3.8+ from https://python.org
    pause
    exit /b 1
)

echo Checking Docker installation...
docker --version >nul 2>&1
if errorlevel 1 (
    echo ERROR: Docker is not installed or not running
    echo Please install Docker Desktop from https://docker.com
    pause
    exit /b 1
)

echo Installing Python dependencies...
pip install pandas neo4j docker pyyaml tqdm

echo Pulling Neo4j Docker image...
docker pull neo4j:community

echo Creating data directory...
if not exist "data" mkdir data

echo Creating results directory...
if not exist "results" mkdir results

echo Creating connection scripts directory...
if not exist "connection_scripts" mkdir connection_scripts

echo Setup completed successfully!
echo.
echo Quick Start:
echo   1. Place your CSV files in the 'data' directory
echo   2. Run: python cross_platform_main_runner.py --interactive
echo   3. Or run: run_kg_builder.bat
echo.
pause
