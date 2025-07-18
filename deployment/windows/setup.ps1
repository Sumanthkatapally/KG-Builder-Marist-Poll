# Neo4j Knowledge Graph Builder - Windows PowerShell Setup
Write-Host "Neo4j Knowledge Graph Builder - Windows Setup" -ForegroundColor Green
Write-Host "=============================================" -ForegroundColor Green

Write-Host "Checking Python installation..." -ForegroundColor Cyan
try {
    $pythonVersion = python --version 2>&1
    Write-Host "[OK] Python found: $pythonVersion" -ForegroundColor Green
} catch {
    Write-Host "[ERROR] Python is not installed or not in PATH" -ForegroundColor Red
    Write-Host "Please install Python 3.8+ from https://python.org" -ForegroundColor Yellow
    Read-Host "Press Enter to exit"
    exit 1
}

Write-Host "Checking Docker installation..." -ForegroundColor Cyan
try {
    $dockerVersion = docker --version 2>&1
    Write-Host "[OK] Docker found: $dockerVersion" -ForegroundColor Green
} catch {
    Write-Host "[ERROR] Docker is not installed or not running" -ForegroundColor Red
    Write-Host "Please install Docker Desktop from https://docker.com" -ForegroundColor Yellow
    Read-Host "Press Enter to exit"
    exit 1
}

Write-Host "Installing Python dependencies..." -ForegroundColor Cyan
pip install pandas neo4j docker pyyaml tqdm

Write-Host "Pulling Neo4j Docker image..." -ForegroundColor Cyan
docker pull neo4j:community

Write-Host "Creating directories..." -ForegroundColor Cyan
if (!(Test-Path "data")) { New-Item -ItemType Directory -Name "data" }
if (!(Test-Path "results")) { New-Item -ItemType Directory -Name "results" }
if (!(Test-Path "connection_scripts")) { New-Item -ItemType Directory -Name "connection_scripts" }

Write-Host "[SUCCESS] Setup completed successfully!" -ForegroundColor Green
Write-Host ""
Write-Host "Quick Start:" -ForegroundColor Yellow
Write-Host "  1. Place your CSV files in the 'data' directory"
Write-Host "  2. Run: python cross_platform_main_runner.py --interactive"
Write-Host "  3. Or double-click: run_kg_builder.bat"
Write-Host ""
Read-Host "Press Enter to continue"
