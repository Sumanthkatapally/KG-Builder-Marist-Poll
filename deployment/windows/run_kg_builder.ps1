# Neo4j Knowledge Graph Builder - Windows PowerShell Runner
Write-Host "Neo4j Knowledge Graph Builder - Windows" -ForegroundColor Green
Write-Host "=======================================" -ForegroundColor Green

# Check if Python files exist
if (!(Test-Path "cross_platform_main_runner.py")) {
    Write-Host "[ERROR] cross_platform_main_runner.py not found" -ForegroundColor Red
    Write-Host "Please ensure all Python files are in this directory" -ForegroundColor Yellow
    Read-Host "Press Enter to exit"
    exit 1
}

# Run the interactive application
python cross_platform_main_runner.py --interactive

Read-Host "Press Enter to continue"
