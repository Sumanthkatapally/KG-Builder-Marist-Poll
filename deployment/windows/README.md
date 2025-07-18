# Neo4j Knowledge Graph Builder - Windows

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
