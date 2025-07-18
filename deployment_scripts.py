#!/usr/bin/env python3
"""
Cross-Platform Deployment Scripts Generator (Windows Compatible)
Creates Windows and Linux deployment packages without Unicode issues
"""

import os
import platform
import json
from pathlib import Path
from typing import Dict, Any

class DeploymentGenerator:
    """Generate deployment scripts for Windows and Linux - Windows Unicode safe"""
    
    def __init__(self):
        self.platform = platform.system().lower()
        self.deployment_dir = Path("deployment")
        self.deployment_dir.mkdir(exist_ok=True)
    
    def create_windows_setup(self) -> Dict[str, str]:
        """Create Windows deployment package - Unicode safe"""
        
        windows_dir = self.deployment_dir / "windows"
        windows_dir.mkdir(exist_ok=True)
        
        scripts = {}
        
        # Windows batch setup script (ASCII only)
        setup_bat = """@echo off
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
"""
        
        # Windows run script (ASCII only)
        run_bat = """@echo off
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
"""
        
        # Windows PowerShell setup script (ASCII only)
        setup_ps1 = """# Neo4j Knowledge Graph Builder - Windows PowerShell Setup
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
"""
        
        # Windows PowerShell run script (ASCII only)
        run_ps1 = """# Neo4j Knowledge Graph Builder - Windows PowerShell Runner
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
"""
        
        # Save Windows scripts with UTF-8 encoding
        scripts['setup_bat'] = str(windows_dir / "setup.bat")
        with open(scripts['setup_bat'], 'w', encoding='utf-8') as f:
            f.write(setup_bat)
        
        scripts['run_bat'] = str(windows_dir / "run_kg_builder.bat")
        with open(scripts['run_bat'], 'w', encoding='utf-8') as f:
            f.write(run_bat)
        
        scripts['setup_ps1'] = str(windows_dir / "setup.ps1")
        with open(scripts['setup_ps1'], 'w', encoding='utf-8') as f:
            f.write(setup_ps1)
        
        scripts['run_ps1'] = str(windows_dir / "run_kg_builder.ps1")
        with open(scripts['run_ps1'], 'w', encoding='utf-8') as f:
            f.write(run_ps1)
        
        # Windows requirements file
        requirements_txt = """# Neo4j Knowledge Graph Builder - Windows Requirements
pandas>=1.3.0
neo4j>=5.0.0
docker>=6.0.0
pyyaml>=6.0.0
tqdm>=4.60.0
"""
        
        scripts['requirements'] = str(windows_dir / "requirements.txt")
        with open(scripts['requirements'], 'w', encoding='utf-8') as f:
            f.write(requirements_txt)
        
        # Windows README (ASCII only)
        windows_readme = """# Neo4j Knowledge Graph Builder - Windows

## Quick Setup

### Option 1: Automatic Setup (Recommended)
1. **Right-click** on `setup.bat` and select "Run as administrator"
2. Follow the prompts to install dependencies
3. Double-click `run_kg_builder.bat` to start

### Option 2: PowerShell Setup
1. **Right-click** on PowerShell and select "Run as administrator"
2. Run: `Set-ExecutionPolicy RemoteSigned` (if needed)
3. Run: `./setup.ps1`
4. Run: `./run_kg_builder.ps1` to start

### Option 3: Manual Setup
1. Install Python 3.8+ from https://python.org
2. Install Docker Desktop from https://docker.com
3. Open Command Prompt as administrator
4. Run: `pip install -r requirements.txt`
5. Run: `docker pull neo4j:community`
6. Run: `python cross_platform_main_runner.py --interactive`

## Directory Structure
```
windows/
├── setup.bat                  # Automatic setup (batch)
├── setup.ps1                  # Automatic setup (PowerShell)
├── run_kg_builder.bat         # Launch application (batch)
├── run_kg_builder.ps1         # Launch application (PowerShell)
├── requirements.txt           # Python dependencies
├── README.md                  # This file
└── data/                      # Place your CSV files here
    ├── survey_ontology.csv    # Your ontology file
    └── survey_data.csv        # Your survey data
```

## Usage Examples

### Command Line
```cmd
# Create single knowledge graph
python cross_platform_main_runner.py --create --survey-name "My Survey" --survey-ontology "data/ontology.csv" --survey-data "data/data.csv"

# List all instances
python cross_platform_main_runner.py --list

# Show platform info
python cross_platform_main_runner.py --platform-info

# Interactive mode
python cross_platform_main_runner.py --interactive
```

### PowerShell
```powershell
# Same commands work in PowerShell
python cross_platform_main_runner.py --interactive
```

## Troubleshooting

### Common Issues
1. **"Python is not recognized"**
   - Install Python from https://python.org
   - Make sure to check "Add Python to PATH" during installation

2. **"Docker is not recognized"**
   - Install Docker Desktop from https://docker.com
   - Make sure Docker Desktop is running

3. **Permission denied errors**
   - Run Command Prompt or PowerShell as administrator

4. **PowerShell execution policy errors**
   - Run: `Set-ExecutionPolicy RemoteSigned` as administrator

### Getting Help
1. Check Windows Event Viewer for errors
2. Run: `python cross_platform_main_runner.py --platform-info`
3. Check Docker Desktop is running
4. Verify CSV files are in the correct format

## Windows-Specific Features
- **Automatic clipboard copying** of Neo4j passwords
- **Double-click connection scripts** to open Neo4j Browser
- **Windows batch and PowerShell scripts** for easy execution
- **Windows path handling** for file inputs
"""
        
        scripts['readme'] = str(windows_dir / "README.md")
        with open(scripts['readme'], 'w', encoding='utf-8') as f:
            f.write(windows_readme)
        
        return scripts
    
    def create_linux_setup(self) -> Dict[str, str]:
        """Create Linux deployment package"""
        
        linux_dir = self.deployment_dir / "linux"
        linux_dir.mkdir(exist_ok=True)
        
        scripts = {}
        
        # Linux setup script
        setup_sh = """#!/bin/bash
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
"""
        
        # Linux run script
        run_sh = """#!/bin/bash
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
"""
        
        # Linux Docker Compose setup (optional)
        docker_compose_sh = """#!/bin/bash
# Neo4j Knowledge Graph Builder - Docker Compose Setup

echo "Setting up Docker Compose environment..."

# Create docker-compose.yml for development
cat > docker-compose.yml << 'EOF'
version: '3.8'
services:
  neo4j-dev:
    image: neo4j:community
    container_name: neo4j-kg-dev
    ports:
      - "7474:7474"
      - "7687:7687"
    environment:
      - NEO4J_AUTH=neo4j/password123
      - NEO4J_dbms_memory_heap_initial__size=1G
      - NEO4J_dbms_memory_heap_max__size=2G
      - NEO4J_dbms_memory_pagecache_size=1G
    volumes:
      - neo4j_dev_data:/data
    restart: unless-stopped

volumes:
  neo4j_dev_data:
EOF

echo "[SUCCESS] Docker Compose configuration created"
echo "To start development Neo4j instance:"
echo "  docker-compose up -d"
echo "To stop:"
echo "  docker-compose down"
"""
        
        # Save Linux scripts with UTF-8 encoding
        scripts['setup_sh'] = str(linux_dir / "setup.sh")
        with open(scripts['setup_sh'], 'w', encoding='utf-8') as f:
            f.write(setup_sh)
        
        scripts['run_sh'] = str(linux_dir / "run_kg_builder.sh")
        with open(scripts['run_sh'], 'w', encoding='utf-8') as f:
            f.write(run_sh)
        
        scripts['docker_compose_sh'] = str(linux_dir / "docker_compose_setup.sh")
        with open(scripts['docker_compose_sh'], 'w', encoding='utf-8') as f:
            f.write(docker_compose_sh)
        
        # Make scripts executable
        for script_path in [scripts['setup_sh'], scripts['run_sh'], scripts['docker_compose_sh']]:
            Path(script_path).chmod(0o755)
        
        # Linux requirements file
        requirements_txt = """# Neo4j Knowledge Graph Builder - Linux Requirements
pandas>=1.3.0
neo4j>=5.0.0
docker>=6.0.0
pyyaml>=6.0.0
tqdm>=4.60.0
"""
        
        scripts['requirements'] = str(linux_dir / "requirements.txt")
        with open(scripts['requirements'], 'w', encoding='utf-8') as f:
            f.write(requirements_txt)
        
        # Linux README
        linux_readme = """# Neo4j Knowledge Graph Builder - Linux

## Quick Setup

### Option 1: Automatic Setup (Recommended)
```bash
chmod +x setup.sh
./setup.sh
./run_kg_builder.sh
```

### Option 2: Manual Setup
```bash
# Install Python and pip (Ubuntu/Debian)
sudo apt update
sudo apt install python3 python3-pip

# Install Docker
curl -fsSL https://get.docker.com -o get-docker.sh
sh get-docker.sh

# Add user to docker group (optional, to avoid sudo)
sudo usermod -aG docker $USER
newgrp docker

# Install Python dependencies
pip3 install --user -r requirements.txt

# Pull Neo4j image
docker pull neo4j:community

# Run application
python3 cross_platform_main_runner.py --interactive
```

## Directory Structure
```
linux/
├── setup.sh                  # Automatic setup script
├── run_kg_builder.sh         # Launch application
├── docker_compose_setup.sh   # Optional Docker Compose setup
├── requirements.txt          # Python dependencies
├── README.md                 # This file
└── data/                     # Place your CSV files here
    ├── survey_ontology.csv   # Your ontology file
    └── survey_data.csv       # Your survey data
```

## Usage Examples

### Command Line
```bash
# Create single knowledge graph
python3 cross_platform_main_runner.py --create \\
    --survey-name "My Survey" \\
    --survey-ontology "data/ontology.csv" \\
    --survey-data "data/data.csv"

# List all instances
python3 cross_platform_main_runner.py --list

# Show platform info
python3 cross_platform_main_runner.py --platform-info

# Interactive mode
python3 cross_platform_main_runner.py --interactive
```

## Troubleshooting

### Common Issues

1. **Permission denied for Docker**
   ```bash
   sudo usermod -aG docker $USER
   newgrp docker
   # Or use sudo with docker commands
   ```

2. **Python module not found**
   ```bash
   pip3 install --user pandas neo4j docker pyyaml tqdm
   # Or use system packages:
   sudo apt install python3-pandas python3-docker
   ```

3. **Docker daemon not running**
   ```bash
   sudo systemctl start docker
   sudo systemctl status docker
   ```

## Linux-Specific Features
- **Automatic clipboard copying** (if xclip/pbcopy available)
- **Shell script integration** for easy automation
- **Advanced Docker management** with scripts
- **Multi-distribution support** (Ubuntu, CentOS, Fedora, etc.)
"""
        
        scripts['readme'] = str(linux_dir / "README.md")
        with open(scripts['readme'], 'w', encoding='utf-8') as f:
            f.write(linux_readme)
        
        return scripts
    
    def create_cross_platform_package(self) -> Dict[str, Any]:
        """Create complete cross-platform deployment package"""
        
        print("Creating Cross-Platform Deployment Package")
        print("=" * 50)
        
        # Create platform-specific packages
        windows_scripts = self.create_windows_setup()
        linux_scripts = self.create_linux_setup()
        
        # Create main deployment README
        main_readme = """# Neo4j Knowledge Graph Builder - Cross-Platform Deployment

## Overview
This package contains everything needed to deploy the Neo4j Knowledge Graph Builder on both Windows and Linux systems.

## Package Structure
```
deployment/
├── windows/                   # Windows deployment package
│   ├── setup.bat             # Windows batch setup
│   ├── setup.ps1             # Windows PowerShell setup
│   ├── run_kg_builder.bat    # Windows batch runner
│   ├── run_kg_builder.ps1    # Windows PowerShell runner
│   ├── requirements.txt      # Python dependencies
│   └── README.md             # Windows-specific instructions
├── linux/                    # Linux deployment package
│   ├── setup.sh              # Linux setup script
│   ├── run_kg_builder.sh     # Linux runner script
│   ├── docker_compose_setup.sh # Optional Docker Compose
│   ├── requirements.txt      # Python dependencies
│   └── README.md             # Linux-specific instructions
└── README.md                 # This file
```

## Quick Start

### Windows Users
1. Copy the `windows/` folder contents to your Windows machine
2. Run `setup.bat` as administrator
3. Double-click `run_kg_builder.bat` to start

### Linux Users
1. Copy the `linux/` folder contents to your Linux machine
2. Run `chmod +x *.sh && ./setup.sh`
3. Run `./run_kg_builder.sh` to start

## Requirements

### Both Platforms
- Python 3.8+
- Docker Engine (Windows: Docker Desktop, Linux: Docker CE)
- 4GB+ RAM recommended
- 10GB+ free disk space

### Platform-Specific
- **Windows**: Windows 10/11, PowerShell 5.1+
- **Linux**: Any modern distribution (Ubuntu, CentOS, Fedora, etc.)

## Support Matrix

| Feature | Windows | Linux | Notes |
|---------|---------|-------|-------|
| Docker Management | [OK] | [OK] | Full support both platforms |
| Auto Port Detection | [OK] | [OK] | Cross-platform compatible |
| Connection Scripts | [OK] | [OK] | Platform-specific formats |
| Clipboard Integration | [OK] | [OK] | Uses platform tools |
| Batch Processing | [OK] | [OK] | Python-based, cross-platform |
| Interactive Mode | [OK] | [OK] | Terminal-based UI |
"""
        
        main_readme_path = self.deployment_dir / "README.md"
        with open(main_readme_path, 'w', encoding='utf-8') as f:
            f.write(main_readme)
        
        # Create deployment info JSON
        deployment_info = {
            'package_name': 'neo4j-knowledge-graph-builder',
            'version': '1.0.0',
            'created_on_platform': self.platform,
            'supported_platforms': ['windows', 'linux'],
            'windows_scripts': windows_scripts,
            'linux_scripts': linux_scripts,
            'main_readme': str(main_readme_path),
            'requirements': {
                'python_version': '3.8+',
                'docker_required': True,
                'memory_recommended': '4GB+',
                'disk_space': '10GB+'
            }
        }
        
        deployment_info_path = self.deployment_dir / "deployment_info.json"
        with open(deployment_info_path, 'w', encoding='utf-8') as f:
            json.dump(deployment_info, f, indent=2)
        
        print(f"[OK] Windows package created: {self.deployment_dir / 'windows'}")
        print(f"[OK] Linux package created: {self.deployment_dir / 'linux'}")
        print(f"[OK] Main README created: {main_readme_path}")
        print(f"[OK] Deployment info: {deployment_info_path}")
        
        return {
            'deployment_dir': str(self.deployment_dir),
            'windows_package': str(self.deployment_dir / 'windows'),
            'linux_package': str(self.deployment_dir / 'linux'),
            'main_readme': str(main_readme_path),
            'deployment_info': deployment_info
        }

def main():
    """Create deployment packages"""
    generator = DeploymentGenerator()
    
    print(f"Creating deployment package on {generator.platform}")
    result = generator.create_cross_platform_package()
    
    print(f"\n[SUCCESS] Deployment Package Created Successfully!")
    print(f"Location: {result['deployment_dir']}")
    print(f"\nNext Steps:")
    
    if generator.platform == 'windows':
        print(f"  Windows: Copy {result['windows_package']} to target Windows machines")
        print(f"  Linux: Copy {result['linux_package']} to target Linux machines")
    else:
        print(f"  Linux: Use {result['linux_package']} on this machine")
        print(f"  Windows: Copy {result['windows_package']} to target Windows machines")
    
    print(f"\nDocumentation:")
    print(f"  Main: {result['main_readme']}")
    print(f"  Windows: {result['windows_package']}/README.md")
    print(f"  Linux: {result['linux_package']}/README.md")

if __name__ == "__main__":
    main()