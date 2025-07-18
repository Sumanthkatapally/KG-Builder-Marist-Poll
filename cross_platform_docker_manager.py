#!/usr/bin/env python3
"""
Flexible Cross-Platform Docker Neo4j Knowledge Graph Builder
Accepts external survey_ontology.py file path and survey_data.csv from command prompt
"""

import os
import sys
import time
import uuid
import json
import logging
import subprocess
import docker
import pandas as pd
import platform
import importlib.util
from pathlib import Path
from typing import Dict, Any, Optional, List
from datetime import datetime
from dataclasses import dataclass
from neo4j_graph_builder import Neo4jSurveyGraphBuilder

@dataclass
class DockerInstance:
    """Represents a Docker Neo4j instance"""
    instance_id: str
    container_name: str
    http_port: int
    bolt_port: int
    password: str
    database_name: str
    volume_name: str
    container_id: Optional[str] = None
    status: str = "created"

class FlexibleCrossPlatformDockerManager:
    """Cross-platform Docker Neo4j manager that accepts external ontology files"""
    
    def __init__(self, base_http_port: int = 7474, base_bolt_port: int = 7687):
        self.docker_client = docker.from_env()
        self.base_http_port = base_http_port
        self.base_bolt_port = base_bolt_port
        self.instances: Dict[str, DockerInstance] = {}
        self.used_ports: set = set()
        self.platform = platform.system().lower()
        
        # Set up logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
        
        self.logger.info(f"Initialized on platform: {self.platform}")
    
    def load_external_ontology(self, ontology_path: str):
        """Dynamically load external survey_ontology.py file"""
        ontology_path = Path(ontology_path)
        
        if not ontology_path.exists():
            raise FileNotFoundError(f"Ontology file not found: {ontology_path}")
        
        if not ontology_path.suffix == '.py':
            raise ValueError(f"Ontology file must be a .py file: {ontology_path}")
        
        try:
            # Load the module dynamically
            spec = importlib.util.spec_from_file_location("survey_ontology", ontology_path)
            ontology_module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(ontology_module)
            
            # Check if SurveyOntology class exists
            if not hasattr(ontology_module, 'SurveyOntology'):
                raise AttributeError(f"SurveyOntology class not found in {ontology_path}")
            
            self.logger.info(f"Successfully loaded ontology from: {ontology_path}")
            return ontology_module.SurveyOntology
            
        except Exception as e:
            self.logger.error(f"Failed to load ontology from {ontology_path}: {e}")
            raise
    
    def _generate_unique_instance_id(self) -> str:
        """Generate unique instance ID"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        unique_suffix = str(uuid.uuid4())[:8]
        return f"neo4j_kg_{timestamp}_{unique_suffix}"
    def start_instance(self, instance_id: str) -> bool:
        """Start a stopped instance"""
        if instance_id not in self.instances:
            self.logger.error(f"Instance {instance_id} not found")
            return False
        
        instance = self.instances[instance_id]
        
        try:
            container = self.docker_client.containers.get(instance.container_id)
            if container.status == 'running':
                self.logger.info(f"Container {instance.container_name} is already running")
                instance.status = "ready"
                return True
            
            container.start()
            instance.status = "starting"
            self.logger.info(f"Started container {instance.container_name}")
            
            # Wait for it to be ready
            if self.wait_for_instance_ready(instance):
                return True
            else:
                self.logger.error(f"Failed to start instance {instance_id}")
                return False
                
        except Exception as e:
            self.logger.error(f"Failed to start container {instance.container_name}: {e}")
            return False
    def discover_existing_instances(self):
        """Discover and load existing Neo4j knowledge graph containers"""
        self.logger.info("Discovering existing Neo4j knowledge graph instances...")
        
        try:
            # Get all containers (running and stopped) that match our naming pattern
            all_containers = self.docker_client.containers.list(all=True)
            discovered_instances = {}
            
            for container in all_containers:
                # Check if container was created by our application
                if container.name.startswith('neo4j-kg-'):
                    try:
                        # Extract information from container
                        container_info = container.attrs
                        
                        # Get ports
                        port_bindings = container_info.get('NetworkSettings', {}).get('Ports', {})
                        http_port = None
                        bolt_port = None
                        
                        if '7474/tcp' in port_bindings and port_bindings['7474/tcp']:
                            http_port = int(port_bindings['7474/tcp'][0]['HostPort'])
                        
                        if '7687/tcp' in port_bindings and port_bindings['7687/tcp']:
                            bolt_port = int(port_bindings['7687/tcp'][0]['HostPort'])
                        
                        # Get environment variables to extract password
                        env_vars = container_info.get('Config', {}).get('Env', [])
                        password = None
                        for env_var in env_vars:
                            if env_var.startswith('NEO4J_AUTH=neo4j/'):
                                password = env_var.split('NEO4J_AUTH=neo4j/')[1]
                                break
                        
                        # Get volume information
                        mounts = container_info.get('Mounts', [])
                        volume_name = None
                        for mount in mounts:
                            if mount.get('Destination') == '/data':
                                volume_name = mount.get('Name') or mount.get('Source', '').split('/')[-1]
                                break
                        
                        # Extract instance ID from container name or generate one
                        # Pattern: neo4j-kg-{survey-name}-{instance_id_suffix}
                        name_parts = container.name.split('-')
                        if len(name_parts) >= 4:
                            instance_id_suffix = name_parts[-1]
                            # Try to find full instance ID from volume name or generate
                            if volume_name and 'neo4j_data_' in volume_name:
                                instance_id = volume_name.replace('neo4j_data_', '')
                            else:
                                # Generate instance ID from container creation time and suffix
                                created_time = container_info.get('Created', '')
                                if created_time:
                                    try:
                                        created_dt = datetime.fromisoformat(created_time.replace('Z', '+00:00'))
                                        instance_id = f"neo4j_kg_{created_dt.strftime('%Y%m%d_%H%M%S')}_{instance_id_suffix}"
                                    except:
                                        instance_id = f"discovered_{container.name}_{instance_id_suffix}"
                                else:
                                    instance_id = f"discovered_{container.name}_{instance_id_suffix}"
                        else:
                            instance_id = f"discovered_{container.name}"
                        
                        # Create DockerInstance object (no import needed, it's in same file)
                        instance = DockerInstance(
                            instance_id=instance_id,
                            container_name=container.name,
                            http_port=http_port or 7474,
                            bolt_port=bolt_port or 7687,
                            password=password or "unknown",
                            database_name="neo4j",  # Default for community edition
                            volume_name=volume_name or f"neo4j_data_{instance_id}",
                            container_id=container.id,
                            status="discovered"
                        )
                        
                        # Update status based on container state
                        if container.status == 'running':
                            instance.status = "ready"
                        elif container.status == 'exited':
                            instance.status = "stopped"
                        else:
                            instance.status = container.status
                        
                        discovered_instances[instance_id] = instance
                        
                        self.logger.info(f"Discovered instance: {container.name} (Status: {instance.status})")
                        
                    except Exception as e:
                        self.logger.warning(f"Error processing container {container.name}: {e}")
                        continue
            
            # Update the instances dictionary
            self.instances.update(discovered_instances)
            
            self.logger.info(f"Discovered {len(discovered_instances)} existing instances")
            return discovered_instances
            
        except Exception as e:
            self.logger.error(f"Error discovering existing instances: {e}")
            return {}    
    def _find_available_ports(self) -> tuple:
        """Find available ports for HTTP and Bolt"""
        http_port = self.base_http_port
        bolt_port = self.base_bolt_port
        
        # Get all currently used ports from existing containers
        existing_containers = self.docker_client.containers.list(all=True)
        used_ports = set()
        
        for container in existing_containers:
            if container.name.startswith('neo4j-kg-'):
                try:
                    ports_info = container.attrs.get('NetworkSettings', {}).get('Ports', {})
                    for port_key, port_mappings in ports_info.items():
                        if port_mappings:
                            for port_mapping in port_mappings:
                                if 'HostPort' in port_mapping:
                                    used_ports.add(int(port_mapping['HostPort']))
                except Exception as e:
                    self.logger.debug(f"Error reading ports from container {container.name}: {e}")
        
        # Find next available ports
        while http_port in used_ports or http_port in self.used_ports:
            http_port += 1
        
        while bolt_port in used_ports or bolt_port in self.used_ports or bolt_port == http_port:
            bolt_port += 1
        
        self.used_ports.add(http_port)
        self.used_ports.add(bolt_port)
        
        return http_port, bolt_port
    
    def create_instance(self, survey_name: str, survey_data_path: str) -> DockerInstance:
        """Create a new Neo4j Docker instance for a survey"""
        
        instance_id = self._generate_unique_instance_id()
        
        # Sanitize survey name for container naming
        safe_survey_name = survey_name.lower().replace(' ', '-').replace('_', '-')
        safe_survey_name = ''.join(c for c in safe_survey_name if c.isalnum() or c in '-')
        
        container_name = f"neo4j-kg-{safe_survey_name}-{instance_id[-8:]}"
        
        # Generate unique password
        password_base = survey_name.lower().replace(' ', '_').replace('-', '_')
        password_base = ''.join(c for c in password_base if c.isalnum() or c == '_')
        password = f"kg_{password_base}"
        
        # Find available ports
        http_port, bolt_port = self._find_available_ports()
        
        # Create unique volume name
        volume_name = f"neo4j_data_{instance_id}"
        
        # Create database name (Neo4j community edition uses 'neo4j' as default)
        database_name = "neo4j"
        
        instance = DockerInstance(
            instance_id=instance_id,
            container_name=container_name,
            http_port=http_port,
            bolt_port=bolt_port,
            password=password,
            database_name=database_name,
            volume_name=volume_name
        )
        
        try:
            self.logger.info(f"Creating Neo4j container: {container_name}")
            self.logger.info(f"  Platform: {self.platform}")
            self.logger.info(f"  HTTP Port: {http_port}")
            self.logger.info(f"  Bolt Port: {bolt_port}")
            self.logger.info(f"  Volume: {volume_name}")
            
            # Platform-specific environment variables
            environment = {
                'NEO4J_AUTH': f'neo4j/{password}',
                'NEO4J_dbms_default__database': database_name,
                'NEO4J_dbms_memory_heap_initial__size': '1G',
                'NEO4J_dbms_memory_heap_max__size': '2G',
                'NEO4J_dbms_memory_pagecache_size': '1G'
            }
            
            # Create Docker container
            container = self.docker_client.containers.run(
                image="neo4j:community",
                name=container_name,
                ports={
                    '7474/tcp': http_port,
                    '7687/tcp': bolt_port
                },
                environment=environment,
                volumes={
                    volume_name: {'bind': '/data', 'mode': 'rw'}
                },
                detach=True,
                restart_policy={'Name': 'unless-stopped'},
                mem_limit='2g' if self.platform != 'windows' else None
            )
            
            instance.container_id = container.id
            instance.status = "starting"
            
            self.instances[instance_id] = instance
            
            self.logger.info(f"Container {container_name} created successfully")
            return instance
            
        except Exception as e:
            self.logger.error(f"Failed to create container {container_name}: {e}")
            raise
    
    def wait_for_instance_ready(self, instance: DockerInstance, timeout: int = 300) -> bool:
        """Wait for Neo4j instance to be ready"""
        
        self.logger.info(f"Waiting for {instance.container_name} to be ready...")
        
        start_time = time.time()
        connection_attempts = 0
        
        while time.time() - start_time < timeout:
            try:
                container = self.docker_client.containers.get(instance.container_id)
                
                # Check if container is running
                if container.status != 'running':
                    self.logger.debug(f"Container status: {container.status}, waiting...")
                    time.sleep(5)
                    continue
                
                connection_attempts += 1
                self.logger.debug(f"Connection attempt {connection_attempts}"+ instance.password)
                self.logger.info(f"DEBUG: Trying to connect with password: {instance.password}")
                
                # Try to connect to Neo4j
                # Import here to avoid circular imports
                
                neo4j_builder = Neo4jSurveyGraphBuilder(
                    uri=f"bolt://localhost:{instance.bolt_port}",
                    username="neo4j",
                    password=instance.password,
                    database=instance.database_name
                )
                
                try:
                    neo4j_builder.connect()
                    # Test with a simple query
                    with neo4j_builder.driver.session(database=instance.database_name) as session:
                        result = session.run("RETURN 1 as test")
                        if result.single()["test"] == 1:
                            neo4j_builder.close()
                            instance.status = "ready"
                            self.logger.info(f"[OK] {instance.container_name} is ready!")
                            return True
                except Exception as conn_e:
                    self.logger.debug(f"Connection attempt failed: {conn_e}")
                    neo4j_builder.close()
                    pass
                
                # Progressive wait times
                if connection_attempts <= 5:
                    time.sleep(5)
                elif connection_attempts <= 10:
                    time.sleep(10)
                else:
                    time.sleep(15)
                
            except Exception as e:
                self.logger.debug(f"Waiting for container: {e}")
                time.sleep(10)
        
        self.logger.error(f"[ERROR] {instance.container_name} failed to become ready within {timeout} seconds")
        instance.status = "failed"
        return False
    
    def get_instance_info(self, instance_id: str) -> Optional[Dict[str, Any]]:
        """Get information about a specific instance"""
        if instance_id not in self.instances:
            return None
        
        instance = self.instances[instance_id]
        
        try:
            container = self.docker_client.containers.get(instance.container_id)
            container_status = container.status
            
            container_info = {
                'created': container.attrs.get('Created', 'Unknown'),
                'image': container.attrs.get('Config', {}).get('Image', 'Unknown'),
                'platform': container.attrs.get('Platform', 'Unknown')
            }
        except:
            container_status = "not_found"
            container_info = {}
        
        return {
            'instance_id': instance.instance_id,
            'container_name': instance.container_name,
            'http_port': instance.http_port,
            'bolt_port': instance.bolt_port,
            'database_name': instance.database_name,
            'volume_name': instance.volume_name,
            'status': instance.status,
            'container_status': container_status,
            'container_info': container_info,
            'neo4j_browser_url': f"http://localhost:{instance.http_port}",
            'bolt_connection': f"bolt://localhost:{instance.bolt_port}",
            'username': 'neo4j',
            'password': instance.password,
            'platform': self.platform
        }
    def save_instance_info(self, instance_id: str):
        """Save instance info including password for reuse"""
        instance_info = self.get_instance_info(instance_id)
        info_file = Path("results") / f"instance_{instance_id}.json"
        
        with open(info_file, 'w') as f:
            json.dump(instance_info, f, indent=2)

    def load_instance_info(self, instance_id: str):
        """Load saved instance info including password"""
        info_file = Path("results") / f"instance_{instance_id}.json"
        if info_file.exists():
            with open(info_file, 'r') as f:
                return json.load(f)
        return None
    
    def generate_connection_scripts(self, instance_id: str) -> Dict[str, str]:
        """Generate platform-specific connection scripts"""
        
        if instance_id not in self.instances:
            return {}
        
        instance_info = self.get_instance_info(instance_id)
        scripts = {}
        
        if self.platform == 'windows':
            # Windows batch file
            batch_script = f"""@echo off
echo Connecting to Neo4j Knowledge Graph: {instance_info['container_name']}
echo =============================================
echo.
echo Neo4j Browser: {instance_info['neo4j_browser_url']}
echo Bolt Connection: {instance_info['bolt_connection']}
echo Username: {instance_info['username']}
echo Password: {instance_info['password']}
echo.
echo Opening Neo4j Browser...
start {instance_info['neo4j_browser_url']}
pause
"""
            
            # PowerShell script
            ps_script = f"""# Neo4j Knowledge Graph Connection Script
Write-Host "Connecting to Neo4j Knowledge Graph: {instance_info['container_name']}" -ForegroundColor Green
Write-Host "=============================================" -ForegroundColor Green
Write-Host ""
Write-Host "Neo4j Browser: {instance_info['neo4j_browser_url']}" -ForegroundColor Cyan
Write-Host "Bolt Connection: {instance_info['bolt_connection']}" -ForegroundColor Cyan
Write-Host "Username: {instance_info['username']}" -ForegroundColor Cyan
Write-Host "Password: {instance_info['password']}" -ForegroundColor Yellow
Write-Host ""

# Copy password to clipboard
Set-Clipboard -Value "{instance_info['password']}"
Write-Host "Password copied to clipboard!" -ForegroundColor Green

# Open Neo4j Browser
Start-Process "{instance_info['neo4j_browser_url']}"

Write-Host ""
Write-Host "Press any key to continue..."
$null = $Host.UI.RawUI.ReadKey("NoEcho,IncludeKeyDown")
"""
            
            scripts['batch'] = batch_script
            scripts['powershell'] = ps_script
        
        else:
            # Linux/macOS shell script
            shell_script = f"""#!/bin/bash
echo "Connecting to Neo4j Knowledge Graph: {instance_info['container_name']}"
echo "============================================="
echo ""
echo "Neo4j Browser: {instance_info['neo4j_browser_url']}"
echo "Bolt Connection: {instance_info['bolt_connection']}"
echo "Username: {instance_info['username']}"
echo "Password: {instance_info['password']}"
echo ""

# Try to copy password to clipboard (if available)
if command -v xclip &> /dev/null; then
    echo "{instance_info['password']}" | xclip -selection clipboard
    echo "Password copied to clipboard (xclip)!"
elif command -v pbcopy &> /dev/null; then
    echo "{instance_info['password']}" | pbcopy
    echo "Password copied to clipboard (pbcopy)!"
fi

# Try to open browser (if available)
if command -v xdg-open &> /dev/null; then
    xdg-open "{instance_info['neo4j_browser_url']}"
elif command -v open &> /dev/null; then
    open "{instance_info['neo4j_browser_url']}"
fi

echo ""
echo "Press Enter to continue..."
read
"""
            
            scripts['shell'] = shell_script
        
        return scripts
    
    def save_connection_scripts(self, instance_id: str, output_dir: str = None) -> Dict[str, str]:
        """Save platform-specific connection scripts to files"""
        
        if output_dir is None:
            output_dir = "connection_scripts"
        
        output_path = Path(output_dir)
        output_path.mkdir(exist_ok=True)
        
        instance_info = self.get_instance_info(instance_id)
        if not instance_info:
            return {}
        
        scripts = self.generate_connection_scripts(instance_id)
        script_files = {}
        
        safe_name = instance_info['container_name'].replace('-', '_')
        
        for script_type, script_content in scripts.items():
            if script_type == 'batch':
                filename = f"connect_{safe_name}.bat"
            elif script_type == 'powershell':
                filename = f"connect_{safe_name}.ps1"
            elif script_type == 'shell':
                filename = f"connect_{safe_name}.sh"
            else:
                continue
            
            file_path = output_path / filename
            
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(script_content)
            
            # Make executable on Unix-like systems
            if script_type == 'shell' and self.platform != 'windows':
                file_path.chmod(0o755)
            
            script_files[script_type] = str(file_path)
        
        return script_files
    
    def list_instances(self) -> List[Dict[str, Any]]:
        """List all managed instances (including discovered ones)"""
        # First, discover any existing instances
        self.discover_existing_instances()
        
        instances_info = []
        
        for instance_id in self.instances.keys():
            info = self.get_instance_info(instance_id)
            if info:
                instances_info.append(info)
        
        return instances_info
    
    def stop_instance(self, instance_id: str) -> bool:
        """Stop a specific instance"""
        if instance_id not in self.instances:
            self.logger.error(f"Instance {instance_id} not found")
            return False
        
        instance = self.instances[instance_id]
        
        try:
            container = self.docker_client.containers.get(instance.container_id)
            container.stop()
            instance.status = "stopped"
            self.logger.info(f"Stopped container {instance.container_name}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to stop container {instance.container_name}: {e}")
            return False
    
    def remove_instance(self, instance_id: str, remove_volume: bool = False) -> bool:
        """Remove a specific instance"""
        if instance_id not in self.instances:
            self.logger.error(f"Instance {instance_id} not found")
            return False
        
        instance = self.instances[instance_id]
        
        try:
            # Stop and remove container
            container = self.docker_client.containers.get(instance.container_id)
            container.stop()
            container.remove()
            
            # Remove volume if requested
            if remove_volume:
                try:
                    volume = self.docker_client.volumes.get(instance.volume_name)
                    volume.remove()
                    self.logger.info(f"Removed volume {instance.volume_name}")
                except:
                    self.logger.warning(f"Could not remove volume {instance.volume_name}")
            
            # Remove from tracking
            del self.instances[instance_id]
            
            # Free up ports
            self.used_ports.discard(instance.http_port)
            self.used_ports.discard(instance.bolt_port)
            
            self.logger.info(f"Removed instance {instance_id}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to remove instance {instance_id}: {e}")
            return False
    
    def cleanup_all_instances(self, remove_volumes: bool = False) -> int:
        """Clean up all managed instances"""
        cleaned_count = 0
        
        for instance_id in list(self.instances.keys()):
            if self.remove_instance(instance_id, remove_volume=remove_volumes):
                cleaned_count += 1
        
        return cleaned_count

class FlexibleCrossPlatformKGBuilder:
    """Flexible cross-platform Knowledge Graph Builder that accepts external ontology files"""
    
    def __init__(self):
        self.docker_manager = FlexibleCrossPlatformDockerManager()
        self.logger = logging.getLogger(__name__)
        
        # Display platform info
        platform_info = self.docker_manager.platform
        self.logger.info(f"Running on {platform_info}")

    def start_knowledge_graph(self, instance_id: str) -> bool:
        """Start a specific knowledge graph instance"""
        return self.docker_manager.start_instance(instance_id)
    
    def validate_input_files(self, survey_ontology_path: str, survey_data_path: str) -> bool:
        """Validate that input files exist and are readable"""
        
        try:
            ontology_path = Path(survey_ontology_path)
            data_path = Path(survey_data_path)
            
            # Check ontology file existence
            if not ontology_path.exists():
                self.logger.error(f"Survey ontology file not found: {ontology_path}")
                return False
            
            if not ontology_path.suffix == '.py':
                self.logger.error(f"Survey ontology must be a .py file: {ontology_path}")
                return False
            
            # Check CSV data file existence
            if not data_path.exists():
                self.logger.error(f"Survey data file not found: {data_path}")
                return False
            
            # Try to read CSV file
            try:
                data_df = pd.read_csv(data_path)
                self.logger.info(f"[OK] Survey data validated: {len(data_df)} rows, {len(data_df.columns)} columns")
            except Exception as e:
                self.logger.error(f"Cannot read survey data file: {e}")
                return False
            
            # Try to load ontology
            try:
                self.docker_manager.load_external_ontology(survey_ontology_path)
                self.logger.info(f"[OK] Survey ontology validated and loaded: {ontology_path}")
            except Exception as e:
                self.logger.error(f"Cannot load survey ontology file: {e}")
                return False
            
            return True
            
        except Exception as e:
            self.logger.error(f"Input file validation failed: {e}")
            return False
    
    def build_knowledge_graph(self, survey_name: str, survey_ontology_path: str, 
                            survey_data_path: str) -> Optional[Dict[str, Any]]:
        """Build knowledge graph with automated Docker instance creation"""
        
        self.logger.info(f"Starting flexible cross-platform knowledge graph creation for: {survey_name}")
        
        # Step 1: Validate input files
        if not self.validate_input_files(survey_ontology_path, survey_data_path):
            return None
        
        # Step 2: Create Docker instance
        self.logger.info("Creating Docker Neo4j instance...")
        try:
            instance = self.docker_manager.create_instance(survey_name, survey_data_path)
        except Exception as e:
            self.logger.error(f"Failed to create Docker instance: {e}")
            return None
        
        # Step 3: Wait for instance to be ready
        if not self.docker_manager.wait_for_instance_ready(instance):
            self.logger.error("Docker instance failed to become ready")
            return None
        
        # Step 4: Build knowledge graph using external ontology
        self.logger.info("Building knowledge graph from CSV data with external ontology...")
        try:
            # Create a custom KG builder class that uses the external ontology
            CustomKGBuilder = self.create_custom_kg_builder(survey_ontology_path)
            
            # Use the custom KG builder with the CSV file path
            kg_builder = CustomKGBuilder(survey_data_path)
            entities, relationships = kg_builder.build_knowledge_graph()
            
            kg_stats = kg_builder.get_statistics()
            self.logger.info(f"[OK] Knowledge graph built: {kg_stats['total_entities']} entities, {kg_stats['total_relationships']} relationships")
            
        except Exception as e:
            self.logger.error(f"Failed to build knowledge graph: {e}")
            return None
        
        # Step 5: Load into Neo4j
        self.logger.info("Loading knowledge graph into Neo4j...")
        try:
            
            neo4j_builder = Neo4jSurveyGraphBuilder(
                uri=f"bolt://localhost:{instance.bolt_port}",
                username="neo4j",
                password=instance.password,
                database=instance.database_name
            )
            
            neo4j_builder.connect()
            neo4j_builder.clear_database(confirm=True)
            neo4j_builder.build_graph_from_entities(entities, relationships)
            
            neo4j_stats = neo4j_builder.get_graph_statistics()
            self.logger.info(f"[OK] Neo4j graph loaded: {neo4j_stats['total_nodes']} nodes, {neo4j_stats['total_relationships']} relationships")
            
            neo4j_builder.close()
            
        except Exception as e:
            self.logger.error(f"Failed to load into Neo4j: {e}")
            return None
        
        # Step 6: Generate connection scripts
        self.logger.info("Generating platform-specific connection scripts...")
        script_files = self.docker_manager.save_connection_scripts(instance.instance_id)
        
        # Step 7: Prepare result
        result = {
            'success': True,
            'survey_name': survey_name,
            'survey_ontology_path': survey_ontology_path,
            'survey_data_path': survey_data_path,
            'instance_info': self.docker_manager.get_instance_info(instance.instance_id),
            'kg_statistics': kg_stats,
            'neo4j_statistics': neo4j_stats,
            'connection_scripts': script_files,
            'platform_info': {'host_platform': self.docker_manager.platform},
            'created_at': datetime.now().isoformat()
        }
        
        self.logger.info(f"[SUCCESS] Flexible cross-platform knowledge graph creation completed successfully!")
        self.logger.info(f"   Neo4j Browser: http://localhost:{instance.http_port}")
        self.logger.info(f"   Bolt Connection: bolt://localhost:{instance.bolt_port}")
        self.logger.info(f"   Username: neo4j")
        self.logger.info(f"   Password: {instance.password}")
        
        if script_files:
            self.logger.info(f"   Connection scripts saved:")
            for script_type, file_path in script_files.items():
                self.logger.info(f"     {script_type}: {file_path}")
        
        return result
    
    def create_custom_kg_builder(self, ontology_path: str):
        """Create a custom KG builder class that uses the external ontology"""
        
        # Load the external ontology
        SurveyOntologyClass = self.docker_manager.load_external_ontology(ontology_path)
        
        # Import the base KG builder classes
        from kg_builder import SurveyKGBuilder, SurveyEntity, SurveyRelationship
        
        # Create a custom class that uses the external ontology
        class CustomSurveyKGBuilder(SurveyKGBuilder):
            def __init__(self, csv_file_path: Optional[str] = None):
                self.csv_file_path = csv_file_path
                self.ontology = SurveyOntologyClass()  # Use external ontology
                self.entities: List[SurveyEntity] = []
                self.relationships: List[SurveyRelationship] = []
                self.respondent_counter = 0
        
        return CustomSurveyKGBuilder
    
    def list_knowledge_graphs(self) -> List[Dict[str, Any]]:
        """List all created knowledge graphs"""
        return self.docker_manager.list_instances()
    
    def stop_knowledge_graph(self, instance_id: str) -> bool:
        """Stop a specific knowledge graph instance"""
        return self.docker_manager.stop_instance(instance_id)
    
    def remove_knowledge_graph(self, instance_id: str, remove_data: bool = False) -> bool:
        """Remove a specific knowledge graph instance"""
        return self.docker_manager.remove_instance(instance_id, remove_volume=remove_data)
    
    def cleanup_all(self, remove_data: bool = False) -> int:
        """Clean up all knowledge graph instances"""
        return self.docker_manager.cleanup_all_instances(remove_volumes=remove_data)

def main():
    """Main entry point for flexible cross-platform knowledge graph creation"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Flexible Cross-Platform Neo4j Knowledge Graph Builder")
    parser.add_argument('--survey-name', required=True, help='Name of the survey')
    parser.add_argument('--survey-ontology', required=True, help='Path to survey ontology Python file')
    parser.add_argument('--survey-data', required=True, help='Path to survey data CSV file')
    parser.add_argument('--list', action='store_true', help='List all knowledge graph instances')
    parser.add_argument('--stop', help='Stop specific instance by ID')
    parser.add_argument('--remove', help='Remove specific instance by ID')
    parser.add_argument('--remove-data', action='store_true', help='Also remove data when removing instance')
    parser.add_argument('--cleanup-all', action='store_true', help='Clean up all instances')
    parser.add_argument('--start', help='Start specific instance by ID')
    
    args = parser.parse_args()
    
    # Initialize builder
    kg_builder = FlexibleCrossPlatformKGBuilder()
    
    if args.list:
        print("Flexible Knowledge Graph Instances:")
        print("=" * 50)
        instances = kg_builder.list_knowledge_graphs()
        
        if not instances:
            print("No instances found.")
        else:
            for instance in instances:
                print(f"Instance ID: {instance['instance_id']}")
                print(f"  Container: {instance['container_name']}")
                print(f"  Status: {instance['status']} / {instance['container_status']}")
                print(f"  Platform: {instance['platform']}")
                print(f"  Neo4j Browser: {instance['neo4j_browser_url']}")
                print(f"  Bolt Connection: {instance['bolt_connection']}")
                print(f"  Username: {instance['username']}")
                print(f"  Password: {instance['password']}")
                print()
        
        return
    
    if args.start:
        print(f"Starting instance: {args.start}")
        success = kg_builder.start_knowledge_graph(args.start)
        if success:
            print("[OK] Instance started successfully")
        else:
            print("[ERROR] Failed to start instance")
        return
    
    if args.stop:
        print(f"Stopping instance: {args.stop}")
        success = kg_builder.stop_knowledge_graph(args.stop)
        if success:
            print("[OK] Instance stopped successfully")
        else:
            print("[ERROR] Failed to stop instance")
        return
    
    if args.remove:
        print(f"Removing instance: {args.remove}")
        success = kg_builder.remove_knowledge_graph(args.remove, remove_data=args.remove_data)
        if success:
            print("[OK] Instance removed successfully")
        else:
            print("[ERROR] Failed to remove instance")
        return
    
    if args.cleanup_all:
        print("Cleaning up all instances...")
        confirm = input("This will remove all knowledge graph instances. Continue? (y/n): ")
        if confirm.lower().startswith('y'):
            count = kg_builder.cleanup_all(remove_data=args.remove_data)
            print(f"[OK] Cleaned up {count} instances")
        else:
            print("Cleanup cancelled")
        return
    
    # Build knowledge graph
    if args.survey_name and args.survey_ontology and args.survey_data:
        print(f"Creating flexible cross-platform knowledge graph for: {args.survey_name}")
        print(f"Using ontology: {args.survey_ontology}")
        print(f"Using data: {args.survey_data}")
        
        result = kg_builder.build_knowledge_graph(
            args.survey_name,
            args.survey_ontology,
            args.survey_data
        )
        
        if result:
            print("[SUCCESS] Flexible cross-platform knowledge graph created successfully!")
            print(f"   Instance ID: {result['instance_info']['instance_id']}")
            print(f"   Platform: {result['platform_info']['host_platform']}")
            print(f"   Neo4j Browser: {result['instance_info']['neo4j_browser_url']}")
            print(f"   Username: {result['instance_info']['username']}")
            print(f"   Password: {result['instance_info']['password']}")
            print(f"   Ontology: {result['survey_ontology_path']}")
            print(f"   Data: {result['survey_data_path']}")
            
            if result['connection_scripts']:
                print(f"   Connection scripts:")
                for script_type, file_path in result['connection_scripts'].items():
                    print(f"     {script_type}: {file_path}")
        else:
            print("[ERROR] Failed to create knowledge graph")
            sys.exit(1)
    else:
        parser.print_help()

if __name__ == "__main__":
    main()