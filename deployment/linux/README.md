# Neo4j Knowledge Graph Builder - Linux

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
python3 cross_platform_main_runner.py --create \
    --survey-name "My Survey" \
    --survey-ontology "data/ontology.csv" \
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
