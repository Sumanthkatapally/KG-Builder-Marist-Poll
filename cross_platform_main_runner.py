#!/usr/bin/env python3
"""
Flexible Cross-Platform Simplified Survey Knowledge Graph Builder - Steps 1-2 Only
Accepts external survey_ontology.py file path and survey_data.csv from command prompt
"""

import sys
from pathlib import Path
import os
import logging
import json
import platform
from typing import Dict, Any, Optional
import pandas as pd

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.absolute()
sys.path.append(str(PROJECT_ROOT))

from cross_platform_docker_manager  import FlexibleCrossPlatformKGBuilder

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("kg_pipeline.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class FlexibleCrossPlatformKGPipeline:
    """Flexible cross-platform pipeline manager that accepts external ontology files"""
    
    def __init__(self):
        self.automated_builder = FlexibleCrossPlatformKGBuilder()
        self.results = {}
        self.platform = platform.system().lower()
        
        logger.info(f"Initialized flexible cross-platform pipeline on {self.platform}")
        
    def validate_input_files(self, survey_ontology_path: str, survey_data_path: str) -> bool:
        """Validate that input files exist and are readable"""
        print("🔍 Validating Input Files")
        print("=" * 50)
        
        try:
            ontology_path = Path(survey_ontology_path)
            data_path = Path(survey_data_path)
            
            # Check ontology file existence
            if not ontology_path.exists():
                print(f"❌ Survey ontology file not found: {ontology_path}")
                return False
            print(f"✓ Survey ontology file found: {ontology_path}")
            
            if not ontology_path.suffix == '.py':
                print(f"❌ Survey ontology must be a .py file: {ontology_path}")
                return False
            print(f"✓ Survey ontology is a Python file")
            
            # Check CSV data file existence
            if not data_path.exists():
                print(f"❌ Survey data file not found: {data_path}")
                return False
            print(f"✓ Survey data file found: {data_path}")
            
            # Try to read CSV file
            try:
                data_df = pd.read_csv(data_path)
                print(f"✓ Survey data loaded: {len(data_df)} rows, {len(data_df.columns)} columns")
            except Exception as e:
                print(f"❌ Cannot read survey data file: {e}")
                return False
            
            # Try to load ontology file
            try:
                self.automated_builder.docker_manager.load_external_ontology(survey_ontology_path)
                print(f"✓ Survey ontology loaded and validated")
            except Exception as e:
                print(f"❌ Cannot load survey ontology: {e}")
                print(f"   Make sure the file contains a 'SurveyOntology' class")
                return False
            
            print("✅ Input file validation completed successfully!")
            return True
            
        except Exception as e:
            print(f"❌ Input file validation failed: {e}")
            return False
    
    def create_knowledge_graph(self, survey_name: str, survey_ontology_path: str, 
                              survey_data_path: str) -> Optional[Dict[str, Any]]:
        """Create complete knowledge graph with cross-platform Docker Neo4j"""
        print(f"\n🚀 Creating Knowledge Graph: {survey_name}")
        print(f"🖥️  Platform: {self.platform.title()}")
        print(f"📄 Ontology: {survey_ontology_path}")
        print(f"📊 Data: {survey_data_path}")
        print("=" * 60)
        
        # Step 1: Validate input files
        if not self.validate_input_files(survey_ontology_path, survey_data_path):
            return None
        
        # Step 2: Create knowledge graph using flexible cross-platform automated builder
        print("\n🔨 Building Knowledge Graph with External Ontology")
        print("-" * 60)
        
        try:
            result = self.automated_builder.build_knowledge_graph(
                survey_name=survey_name,
                survey_ontology_path=survey_ontology_path,
                survey_data_path=survey_data_path
            )
            
            if result and result['success']:
                print("\n✅ Flexible Cross-Platform Knowledge Graph Creation Completed!")
                print("=" * 60)
                
                # Display summary
                instance_info = result['instance_info']
                kg_stats = result['kg_statistics']
                neo4j_stats = result['neo4j_statistics']
                platform_info = result['platform_info']
                connection_scripts = result['connection_scripts']
                
                print(f"📊 Knowledge Graph Statistics:")
                print(f"  • Total Entities: {kg_stats['total_entities']}")
                print(f"  • Total Relationships: {kg_stats['total_relationships']}")
                print(f"  • Neo4j Nodes: {neo4j_stats['total_nodes']}")
                print(f"  • Neo4j Relationships: {neo4j_stats['total_relationships']}")
                
                print(f"\n📁 Input Files:")
                print(f"  • Ontology: {result['survey_ontology_path']}")
                print(f"  • Data: {result['survey_data_path']}")
                
                print(f"\n🐳 Docker Instance Information:")
                print(f"  • Instance ID: {instance_info['instance_id']}")
                print(f"  • Container Name: {instance_info['container_name']}")
                print(f"  • Status: {instance_info['status']}")
                print(f"  • Platform: {instance_info['platform']}")
                
                print(f"\n🌐 Connection Information:")
                print(f"  • Neo4j Browser: {instance_info['neo4j_browser_url']}")
                print(f"  • Bolt Connection: {instance_info['bolt_connection']}")
                print(f"  • Username: {instance_info['username']}")
                print(f"  • Password: {instance_info['password']}")
                
                # Platform-specific connection instructions
                if connection_scripts:
                    print(f"\n📜 Platform-Specific Connection Scripts:")
                    for script_type, file_path in connection_scripts.items():
                        print(f"  • {script_type.title()}: {file_path}")
                    
                    if self.platform == 'windows':
                        print(f"\n💡 Windows Quick Start:")
                        if 'batch' in connection_scripts:
                            print(f"  • Double-click: {connection_scripts['batch']}")
                        if 'powershell' in connection_scripts:
                            print(f"  • PowerShell: {connection_scripts['powershell']}")
                    else:
                        print(f"\n💡 Linux/macOS Quick Start:")
                        if 'shell' in connection_scripts:
                            print(f"  • Run: {connection_scripts['shell']}")
                
                print(f"\n📈 Entity Type Breakdown:")
                for entity_type, count in kg_stats['entity_type_counts'].items():
                    print(f"  • {entity_type}: {count}")
                
                print(f"\n🔗 Relationship Type Breakdown:")
                for rel_type, count in kg_stats['relationship_type_counts'].items():
                    print(f"  • {rel_type}: {count}")
                
                # Export results
                self.export_results(result)
                
                return result
            else:
                print("❌ Knowledge graph creation failed")
                return None
                
        except Exception as e:
            logger.error(f"Knowledge graph creation failed: {e}")
            print(f"❌ Knowledge graph creation failed: {e}")
            return None
    
    def export_results(self, result: Dict[str, Any]) -> str:
        """Export pipeline results to JSON file with platform info"""
        try:
            # Create results directory if it doesn't exist
            results_dir = Path("results")
            results_dir.mkdir(exist_ok=True)
            
            # Create filename with timestamp and platform
            timestamp = pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')
            platform_suffix = self.platform[:3]  # win, lin, dar
            filename = f"kg_creation_result_{result['survey_name']}_{platform_suffix}_{timestamp}.json"
            output_path = results_dir / filename
            
            # Prepare export data
            export_data = {
                'survey_name': result['survey_name'],
                'creation_timestamp': result['created_at'],
                'success': result['success'],
                'platform': self.platform,
                'input_files': {
                    'survey_ontology_path': result['survey_ontology_path'],
                    'survey_data_path': result['survey_data_path']
                },
                'docker_instance': result['instance_info'],
                'knowledge_graph_statistics': result['kg_statistics'],
                'neo4j_statistics': result['neo4j_statistics'],
                'platform_info': result['platform_info'],
                'connection_scripts': result['connection_scripts'],
                'access_instructions': {
                    'neo4j_browser': result['instance_info']['neo4j_browser_url'],
                    'bolt_connection': result['instance_info']['bolt_connection'],
                    'username': result['instance_info']['username'],
                    'password': result['instance_info']['password']
                }
            }
            
            # Write to file
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(export_data, f, indent=2, default=str)
            
            print(f"📄 Results exported to: {output_path}")
            return str(output_path)
            
        except Exception as e:
            logger.error(f"Failed to export results: {e}")
            print(f"❌ Failed to export results: {e}")
            return ""
    
    def list_knowledge_graphs(self) -> None:
        """List all created knowledge graphs with platform info"""
        print("\n📋 Existing Cross-Platform Knowledge Graph Instances")
        print("=" * 60)
        
        instances = self.automated_builder.list_knowledge_graphs()
        
        if not instances:
            print("No knowledge graph instances found.")
            return
        
        for i, instance in enumerate(instances, 1):
            print(f"{i}. {instance['container_name']}")
            print(f"   Instance ID: {instance['instance_id']}")
            print(f"   Platform: {instance['platform']}")
            print(f"   Status: {instance['status']} / {instance['container_status']}")
            print(f"   Neo4j Browser: {instance['neo4j_browser_url']}")
            print(f"   Bolt: {instance['bolt_connection']}")
            print(f"   Username: {instance['username']}")
            print(f"   Password: {instance['password']}")
            print()
    
    def manage_instance(self, action: str, instance_id: str = None) -> bool:
        """Manage knowledge graph instances with cross-platform support"""
        if action == "start" and instance_id:
            print(f"🚀 Starting instance: {instance_id}")
            success = self.automated_builder.start_knowledge_graph(instance_id)
            if success:
                print("✅ Instance started successfully")
            else:
                print("❌ Failed to start instance")
            return success
        
        elif action == "stop" and instance_id:
            print(f"🛑 Stopping instance: {instance_id}")
            success = self.automated_builder.stop_knowledge_graph(instance_id)
            if success:
                print("✅ Instance stopped successfully")
            else:
                print("❌ Failed to stop instance")
            return success
        
        # ... rest of your existing code stays the same    
    def interactive_menu(self):
        """Interactive menu for cross-platform pipeline operations"""
        while True:
            print(f"\n🔧 Flexible Cross-Platform Knowledge Graph Builder - {self.platform.title()}")
            print("=" * 70)
            print("1. Create new knowledge graph")
            print("2. List existing knowledge graphs")
            print("3. Stop knowledge graph instance")
            print("4. Remove knowledge graph instance")
            print("5. Cleanup all instances")
            print("6. Exit")
            
            try:
                choice = input(f"\nEnter choice (1-6): ").strip()
                
                if choice == '1':
                    self.create_knowledge_graph_interactive()
                elif choice == '2':
                    self.list_knowledge_graphs()
                elif choice == '3':
                    self.stop_instance_interactive()
                elif choice == '4':
                    self.remove_instance_interactive()
                elif choice == '5':
                    self.manage_instance("cleanup_all")
                elif choice == '6':
                    print("Goodbye!")
                    break
                else:
                    print("Invalid choice. Please enter 1-6.")
                    
            except KeyboardInterrupt:
                print("\nGoodbye!")
                break
            except Exception as e:
                print(f"Error: {e}")
    
    def create_knowledge_graph_interactive(self):
        """Interactive knowledge graph creation with platform-specific guidance"""
        print(f"\n📝 Create New Knowledge Graph on {self.platform.title()}")
        print("-" * 50)
        
        try:
            survey_name = input("Enter survey name: ").strip()
            if not survey_name:
                print("❌ Survey name cannot be empty")
                return
            
            # Platform-specific file path input guidance
            if self.platform == 'windows':
                print("💡 Windows tip: Use forward slashes (/) or double backslashes (\\\\) in paths")
                print("   Example: C:/ontology/survey_ontology.py")
            else:
                print("💡 Linux/macOS tip: Use absolute paths starting with / or relative paths")
                print("   Example: /home/user/ontology/survey_ontology.py")
            
            survey_ontology_path = input("Enter path to survey ontology Python file (.py): ").strip()
            if not survey_ontology_path:
                print("❌ Survey ontology path cannot be empty")
                return
            
            survey_data_path = input("Enter path to survey data CSV file (.csv): ").strip()
            if not survey_data_path:
                print("❌ Survey data path cannot be empty")
                return
            
            # Create knowledge graph
            result = self.create_knowledge_graph(
                survey_name=survey_name,
                survey_ontology_path=survey_ontology_path,
                survey_data_path=survey_data_path
            )
            
            if result:
                print(f"\n🎉 Successfully created knowledge graph for '{survey_name}' on {self.platform.title()}!")
                
                # Platform-specific next steps
                if self.platform == 'windows':
                    print("\n💡 Windows Next Steps:")
                    print("  • Double-click the .bat file to connect")
                    print("  • Or run the .ps1 file in PowerShell")
                    print("  • Password will be copied to clipboard automatically")
                else:
                    print("\n💡 Linux/macOS Next Steps:")
                    print("  • Run the .sh script to connect")
                    print("  • Or open the Neo4j Browser URL manually")
                    print("  • Password will be copied to clipboard if available")
                
            else:
                print(f"\n❌ Failed to create knowledge graph for '{survey_name}'")
                
        except KeyboardInterrupt:
            print("\nOperation cancelled")
        except Exception as e:
            print(f"Error: {e}")
    
    def stop_instance_interactive(self):
        """Interactive instance stopping"""
        print(f"\n🛑 Stop Knowledge Graph Instance on {self.platform.title()}")
        print("-" * 50)
        
        # List instances first
        instances = self.automated_builder.list_knowledge_graphs()
        if not instances:
            print("No instances found.")
            return
        
        print("Available instances:")
        for i, instance in enumerate(instances, 1):
            status = instance['status']
            platform_info = instance.get('platform', 'unknown')
            print(f"{i}. {instance['container_name']} ({status} - {platform_info})")
        
        try:
            choice = input(f"\nEnter instance number to stop (1-{len(instances)}): ").strip()
            instance_idx = int(choice) - 1
            
            if 0 <= instance_idx < len(instances):
                instance_id = instances[instance_idx]['instance_id']
                self.manage_instance("stop", instance_id)
            else:
                print("Invalid selection")
                
        except (ValueError, KeyboardInterrupt):
            print("Operation cancelled")
        except Exception as e:
            print(f"Error: {e}")
    
    def remove_instance_interactive(self):
        """Interactive instance removal"""
        print(f"\n🗑️ Remove Knowledge Graph Instance on {self.platform.title()}")
        print("-" * 50)
        
        # List instances first
        instances = self.automated_builder.list_knowledge_graphs()
        if not instances:
            print("No instances found.")
            return
        
        print("Available instances:")
        for i, instance in enumerate(instances, 1):
            status = instance['status']
            platform_info = instance.get('platform', 'unknown')
            print(f"{i}. {instance['container_name']} ({status} - {platform_info})")
        
        try:
            choice = input(f"\nEnter instance number to remove (1-{len(instances)}): ").strip()
            instance_idx = int(choice) - 1
            
            if 0 <= instance_idx < len(instances):
                instance_id = instances[instance_idx]['instance_id']
                self.manage_instance("remove", instance_id)
            else:
                print("Invalid selection")
                
        except (ValueError, KeyboardInterrupt):
            print("Operation cancelled")
        except Exception as e:
            print(f"Error: {e}")


def main():
    """Main entry point with flexible cross-platform command line interface"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Flexible Cross-Platform Survey Knowledge Graph Builder (Steps 1-2)")
    
    # Primary action arguments
    parser.add_argument('--create', action='store_true', help='Create new knowledge graph')
    parser.add_argument('--survey-name', help='Name of the survey')
    parser.add_argument('--survey-ontology', help='Path to survey ontology Python file (.py)')
    parser.add_argument('--survey-data', help='Path to survey data CSV file (.csv)')
    
    # Management arguments
    parser.add_argument('--list', action='store_true', help='List all knowledge graph instances')
    parser.add_argument('--stop', help='Stop specific instance by ID')
    parser.add_argument('--remove', help='Remove specific instance by ID')
    parser.add_argument('--cleanup-all', action='store_true', help='Clean up all instances')
    parser.add_argument('--start', help='Start specific instance by ID')
    
    # Interactive mode
    parser.add_argument('--interactive', action='store_true', help='Run in interactive mode')
    
    args = parser.parse_args()
    
    # Initialize pipeline
    pipeline = FlexibleCrossPlatformKGPipeline()
    current_platform = pipeline.platform
    
    try:
        if args.create:
            # Create knowledge graph
            if not all([args.survey_name, args.survey_ontology, args.survey_data]):
                print("❌ For --create, you must provide --survey-name, --survey-ontology, and --survey-data")
                print("   Example: --survey-name 'My Survey' --survey-ontology 'path/to/survey_ontology.py' --survey-data 'path/to/data.csv'")
                sys.exit(1)
            
            result = pipeline.create_knowledge_graph(
                survey_name=args.survey_name,
                survey_ontology_path=args.survey_ontology,
                survey_data_path=args.survey_data
            )
            
            sys.exit(0 if result else 1)
            
        elif args.start:
            # Start instance
            success = pipeline.manage_instance("start", args.start)
            sys.exit(0 if success else 1)
        
        elif args.list:
            # List instances
            pipeline.list_knowledge_graphs()
            sys.exit(0)
        
        elif args.stop:
            # Stop instance
            success = pipeline.manage_instance("stop", args.stop)
            sys.exit(0 if success else 1)
        
        elif args.remove:
            # Remove instance
            success = pipeline.manage_instance("remove", args.remove)
            sys.exit(0 if success else 1)
        
        elif args.cleanup_all:
            # Cleanup all instances
            success = pipeline.manage_instance("cleanup_all")
            sys.exit(0 if success else 1)
        
        elif args.interactive:
            # Interactive mode
            pipeline.interactive_menu()
            sys.exit(0)
        
        else:
            # Default: show help and run interactive mode
            print(f"Flexible Cross-Platform Survey Knowledge Graph Builder - {current_platform.title()}")
            print("=" * 75)
            print("Accepts external survey_ontology.py and survey_data.csv file paths")
            print("No arguments provided. Running in interactive mode...")
            print("Use --help to see all available options.")
            print()
            pipeline.interactive_menu()
    
    except KeyboardInterrupt:
        print("\nOperation cancelled by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Application error: {e}")
        print(f"❌ Application error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()