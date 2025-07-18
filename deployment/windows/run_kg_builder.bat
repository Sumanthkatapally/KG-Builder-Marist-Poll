@echo off
echo Neo4j Knowledge Graph Builder - Windows
echo =======================================

REM Check if Python files exist
if not exist "cross_platform_main_runner.py" (
    echo ERROR: cross_platform_main_runner.py not found
    echo Please ensure all Python files are in this directory
    pause
    exit /b 1
)

REM Run the interactive application
python cross_platform_main_runner.py --interactive

pause
