#!/bin/bash
# Neo4j Knowledge Graph Builder - Linux Setup

echo "Neo4j Knowledge Graph Builder - Linux Setup"
echo "==========================================="

# Check Python installation
echo "Checking Python installation..."
if ! command -v python3 &> /dev/null; then
    echo "[ERROR] Python 3 is not installed"
    echo "Please install Python 3.8+:"
    echo "  Ubuntu/Debian: sudo apt update && sudo apt install python3 python3-pip"
    echo "  CentOS/RHEL: sudo yum install python3 python3-pip"
    echo "  Fedora: sudo dnf install python3 python3-pip"
    exit 1
fi

PYTHON_VERSION=$(python3 --version)
echo "[OK] Python found: $PYTHON_VERSION"

# Check pip installation
if ! command -v pip3 &> /dev/null; then
    echo "[ERROR] pip3 is not installed"
    echo "Please install pip3:"
    echo "  Ubuntu/Debian: sudo apt install python3-pip"
    echo "  CentOS/RHEL: sudo yum install python3-pip"
    exit 1
fi

# Check Docker installation
echo "Checking Docker installation..."
if ! command -v docker &> /dev/null; then
    echo "[ERROR] Docker is not installed"
    echo "Please install Docker:"
    echo "  Ubuntu/Debian: https://docs.docker.com/engine/install/ubuntu/"
    echo "  CentOS/RHEL: https://docs.docker.com/engine/install/centos/"
    echo "  Or run: curl -fsSL https://get.docker.com -o get-docker.sh && sh get-docker.sh"
    exit 1
fi

DOCKER_VERSION=$(docker --version)
echo "[OK] Docker found: $DOCKER_VERSION"

# Check if Docker daemon is running
if ! docker info &> /dev/null; then
    echo "[ERROR] Docker daemon is not running"
    echo "Please start Docker:"
    echo "  sudo systemctl start docker"
    echo "  sudo systemctl enable docker"
    exit 1
fi

# Check Docker permissions
if ! docker ps &> /dev/null; then
    echo "[WARNING] User cannot access Docker without sudo"
    echo "To fix this, add your user to the docker group:"
    echo "  sudo usermod -aG docker $USER"
    echo "  newgrp docker"
    echo ""
    echo "For now, you may need to run commands with sudo"
fi

# Install Python dependencies
echo "Installing Python dependencies..."
pip3 install --user pandas neo4j docker pyyaml tqdm

# Pull Neo4j Docker image
echo "Pulling Neo4j Docker image..."
docker pull neo4j:community

# Create directories
echo "Creating directories..."
mkdir -p data
mkdir -p results
mkdir -p connection_scripts

# Make scripts executable
chmod +x *.sh

echo "[SUCCESS] Setup completed successfully!"
echo ""
echo "Quick Start:"
echo "  1. Place your CSV files in the 'data' directory"
echo "  2. Run: python3 cross_platform_main_runner.py --interactive"
echo "  3. Or run: ./run_kg_builder.sh"
echo ""
