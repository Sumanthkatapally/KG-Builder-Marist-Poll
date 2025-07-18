# Neo4j Knowledge Graph Builder - Cross-Platform Deployment

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
