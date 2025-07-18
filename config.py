import os
from pathlib import Path
from typing import Optional

class Config:
    """Configuration settings for the survey knowledge graph system"""
    
    # Project paths
    PROJECT_ROOT = Path(__file__).parent.absolute()
    DATA_DIR = PROJECT_ROOT / "data"
    RESULTS_DIR = PROJECT_ROOT / "results"
    LOGS_DIR = PROJECT_ROOT / "logs"
    
    # Create directories if they don't exist
    DATA_DIR.mkdir(exist_ok=True)
    RESULTS_DIR.mkdir(exist_ok=True)
    LOGS_DIR.mkdir(exist_ok=True)
    
    # Neo4j Configuration (will be overridden by Docker manager)
    NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
    NEO4J_USERNAME = os.getenv("NEO4J_USERNAME", "neo4j")
    NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "password")
    NEO4J_DATABASE = os.getenv("NEO4J_DATABASE", "neo4j")
    
    # Survey data configuration  
    CSV_FILE_PATH = DATA_DIR / "survey_data.csv"
    
    # Batch processing
    BATCH_SIZE = 1000
    RELATIONSHIP_BATCH_SIZE = 500
    
    # Logging
    LOG_LEVEL = "INFO"
    LOG_FILE = LOGS_DIR / "kg_pipeline.log"
    
    @classmethod
    def validate_config(cls) -> bool:
        """Validate configuration settings"""
        # For the pipeline, we don't need to validate Neo4j settings here
        # since they're managed by the Docker manager
        return True