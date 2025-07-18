#!/bin/bash
# Neo4j Knowledge Graph Builder - Linux Runner

echo "Neo4j Knowledge Graph Builder - Linux"
echo "====================================="

# Check if Python files exist
if [ ! -f "cross_platform_main_runner.py" ]; then
    echo "[ERROR] cross_platform_main_runner.py not found"
    echo "Please ensure all Python files are in this directory"
    exit 1
fi

# Run the interactive application
python3 cross_platform_main_runner.py --interactive
