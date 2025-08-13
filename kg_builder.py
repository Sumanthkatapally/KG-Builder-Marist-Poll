#kg_builder.py
import pandas as pd
import logging
import importlib.util
from typing import Dict, List, Tuple, Any, Optional
from dataclasses import dataclass
from datetime import datetime
import uuid
from pathlib import Path
from config import Config

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(Config.LOG_FILE),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

@dataclass
class SurveyEntity:
    """Represents a survey entity with enhanced properties"""
    entity_id: str
    entity_type: str
    properties: Dict[str, Any]
    text_content: Optional[str] = None  # For vectorization
    
@dataclass 
class SurveyRelationship:
    """Represents a relationship between entities"""
    source_id: str
    target_id: str
    relationship_type: str
    properties: Dict[str, Any] = None

class SurveyKGBuilder:
    """Enhanced knowledge graph builder for survey data with external ontology support"""
    
    def __init__(self, csv_file_path: Optional[str] = None, ontology_path: Optional[str] = None):
        self.csv_file_path = csv_file_path or Config.CSV_FILE_PATH
        self.ontology_path = ontology_path
        
        # Load ontology - either external or default
        if ontology_path:
            self.ontology = self._load_external_ontology(ontology_path)
        else:
            # Try to import default ontology
            try:
                from survey_ontology import SurveyOntology
                self.ontology = SurveyOntology()
            except ImportError:
                raise ImportError("No ontology specified and default survey_ontology.py not found. Please provide ontology_path parameter.")
        
        self.entities: List[SurveyEntity] = []
        self.relationships: List[SurveyRelationship] = []
        self.respondent_counter = 0
    
    def _load_external_ontology(self, ontology_path: str):
        """Load external survey_ontology.py file"""
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
            
            logger.info(f"Successfully loaded ontology from: {ontology_path}")
            return ontology_module.SurveyOntology()
            
        except Exception as e:
            logger.error(f"Failed to load ontology from {ontology_path}: {e}")
            raise
        
    def generate_unique_respondent_id(self, row_index: int) -> str:
        """Generate unique respondent ID in format: sequential_number + date + time + seconds"""
        now = datetime.now()
        # Format: NNNNYYYYMMDDHHMMSS (sequential + timestamp)
        unique_id = f"{self.respondent_counter:06d}{now.strftime('%Y%m%d%H%M%S')}"
        self.respondent_counter += 1
        return unique_id
    
    def load_and_validate_data(self) -> pd.DataFrame:
        """Load CSV data and validate against ontology"""
        try:
            df = pd.read_csv(self.csv_file_path)
            logger.info(f"Loaded {len(df)} respondents from CSV: {self.csv_file_path}")
            
            # Validate columns
            validation = self.ontology.validate_csv_fields(df.columns.tolist())
            
            logger.info(f"Known fields: {validation['total_known']}")
            logger.info(f"Unknown fields: {validation['total_unknown']}")
            
            if validation['unknown_fields']:
                logger.warning(f"Unknown fields found: {validation['unknown_fields'][:10]}...")  # Show first 10
            
            if validation['missing_fields']:
                logger.warning(f"Expected fields missing: {validation['missing_fields'][:10]}...")  # Show first 10
                
            return df
            
        except Exception as e:
            logger.error(f"Error loading CSV: {e}")
            raise
    
    def safe_get_value(self, row: pd.Series, column: str, default: Any = None) -> Any:
        """Safely get value from row with None handling"""
        try:
            value = row.get(column, default)
            if pd.isna(value) or value == '' or str(value).strip() == '':
                return None
            # Convert numpy types to Python native types
            if hasattr(value, 'item'):  # numpy scalar
                value = value.item()
            return str(value).strip()
        except Exception:
            return None
    
    def extract_respondent_entities(self, df: pd.DataFrame) -> None:
        """Extract respondent entities with unique ID generation"""
        self.respondent_id_mapping = {}  # Store mapping of row index to respondent ID
        
        for index, row in df.iterrows():
            # Generate unique respondent ID
            respondent_id = f"{self.respondent_counter:06d}{datetime.now().strftime('%Y%m%d%H%M%S')}"
            
            # Store mapping for consistent relationship creation
            self.respondent_id_mapping[index] = respondent_id
            self.respondent_counter += 1
            
            # Create main respondent entity with safe value extraction
            respondent_properties = {
                'unique_id': respondent_id,
                'original_uid': self._safe_convert_value(row.get('UID')) or respondent_id,
                'survey_project': self._safe_convert_value(row.get('PROJECT_NAME')) or 'unknown',
                'created_at': self._safe_convert_value(row.get('YEARRAW')) or str(datetime.now().year),
                'row_index': int(index)
            }
            
            # Generate text content for respondent
            text_content = f"Respondent {respondent_id} from survey {respondent_properties['survey_project']}"
            
            respondent_entity = SurveyEntity(
                entity_id=respondent_id,
                entity_type='Respondent',
                properties=respondent_properties,
                text_content=text_content
            )
            self.entities.append(respondent_entity)
    
    def _safe_convert_value(self, value: Any) -> Any:
        """Safely convert numpy and pandas types to Python native types"""
        if value is None or pd.isna(value):
            return None
        
        # Handle numpy types
        if hasattr(value, 'item'):
            try:
                value = value.item()
            except (ValueError, AttributeError):
                pass
        
        # Convert to string and strip
        try:
            str_value = str(value).strip()
            if str_value == '' or str_value.lower() in ['nan', 'none', 'null']:
                return None
            return str_value
        except:
            return None
    
    def extract_response_entities(self, df: pd.DataFrame) -> None:
        """Extract response category entities with proper connection handling"""
        
        for index, row in df.iterrows():
            # Use the same respondent ID from mapping
            respondent_id = self.respondent_id_mapping.get(index)
            if not respondent_id:
                logger.warning(f"No respondent ID found for row {index}")
                continue
            
            # Process each response category
            for relationship_type, field_set in self.ontology.RELATIONSHIP_MAPPINGS.items():
                category_responses = {}
                
                # Collect all responses for this category with safe value conversion
                for field in field_set:
                    if field in row.index:
                        value = self._safe_convert_value(row[field])
                        # Use descriptive field name instead of original code
                        descriptive_name = self.ontology.get_descriptive_field_name(field)
                        category_responses[descriptive_name] = value
                
                # Only create entity if there are non-None responses
                non_null_responses = {k: v for k, v in category_responses.items() if v is not None}
                
                if non_null_responses:
                    # Create response entity for this category
                    response_entity_id = f"{respondent_id}_{relationship_type.lower()}"
                    category_name = self.ontology.get_category_class_name(relationship_type)
                    
                    # Add metadata to response properties
                    response_properties = {
                        'respondent_uid': respondent_id,
                        'response_category': relationship_type,
                        'response_count': len(non_null_responses),
                        **category_responses  # Include all responses (with descriptive names)
                    }
                    
                    # Generate text content for this response category
                    text_content = self._create_category_text_content(
                        category_name, non_null_responses, respondent_id
                    )
                    
                    response_entity = SurveyEntity(
                        entity_id=response_entity_id,
                        entity_type=category_name,
                        properties=response_properties,
                        text_content=text_content
                    )
                    self.entities.append(response_entity)
                    
                    # Create relationship between respondent and response category
                    relationship = SurveyRelationship(
                        source_id=respondent_id,
                        target_id=response_entity_id,
                        relationship_type=relationship_type,
                        properties={
                            'respondent_uid': respondent_id,
                            'response_count': len(non_null_responses),
                            'created_at': datetime.now().isoformat()
                        }
                    )
                    self.relationships.append(relationship)
    
    def extract_question_entities(self, df: pd.DataFrame) -> None:
        """Extract individual question entities with descriptions"""
        processed_questions = set()
        
        for column in df.columns:
            if column.upper() in self.ontology.get_all_survey_fields():
                if column not in processed_questions:
                    # Use descriptive field name for the entity
                    descriptive_name = self.ontology.get_descriptive_field_name(column)
                    
                    question_properties = {
                        'original_field_name': column,
                        'descriptive_field_name': descriptive_name,
                        'category': self.ontology.get_category_for_field(column),
                        'description': self.ontology.get_field_description(column),
                        'data_type': str(df[column].dtype),
                        'unique_values': int(df[column].nunique()),
                        'null_count': int(df[column].isnull().sum())
                    }
                    
                    text_content = f"Survey question {descriptive_name}: {question_properties['description']} in {question_properties['category']} category"
                    
                    question_entity = SurveyEntity(
                        entity_id=f"question_{descriptive_name}",
                        entity_type='Question',
                        properties=question_properties,
                        text_content=text_content
                    )
                    self.entities.append(question_entity)
                    processed_questions.add(column)
    
    def extract_survey_metadata(self, df: pd.DataFrame) -> None:
        """Extract survey-level metadata with enhanced statistics"""
        if 'PROJECT_NAME' in df.columns:
            project_names = df['PROJECT_NAME'].dropna().unique()
            
            for project_name in project_names:
                # Convert numpy types to Python string
                if hasattr(project_name, 'item'):
                    project_name = project_name.item()
                project_name = str(project_name)
                
                project_df = df[df['PROJECT_NAME'] == project_name]
                
                # Safe extraction of year with proper type conversion
                year_value = 'unknown'
                if len(project_df) > 0 and 'YEARRAW' in project_df.columns:
                    first_year = project_df.iloc[0]['YEARRAW']
                    if pd.notna(first_year):
                        if hasattr(first_year, 'item'):
                            first_year = first_year.item()
                        year_value = str(first_year)
                
                survey_properties = {
                    'project_name': project_name,
                    'total_respondents': int(len(project_df)),
                    'survey_year': year_value,
                    'completion_rate': round((len(project_df) / len(df)) * 100, 2),
                    'data_quality_score': self._calculate_data_quality_score(project_df)
                }
                
                text_content = f"Survey {project_name} with {survey_properties['total_respondents']} respondents from {survey_properties['survey_year']}"
                
                # Create safe entity ID
                safe_project_name = str(project_name).lower().replace(' ', '_').replace('-', '_')
                
                survey_entity = SurveyEntity(
                    entity_id=f"survey_{safe_project_name}",
                    entity_type='Survey',
                    properties=survey_properties,
                    text_content=text_content
                )
                self.entities.append(survey_entity)
                
                # Create relationships between survey and respondents using the mapping
                for index, row in project_df.iterrows():
                    respondent_id = self.respondent_id_mapping.get(index)
                    if respondent_id:
                        relationship = SurveyRelationship(
                            source_id=f"survey_{safe_project_name}",
                            target_id=respondent_id,
                            relationship_type='HAS_RESPONDENT',
                            properties={'survey_project': project_name}
                        )
                        self.relationships.append(relationship)
    
    def _create_category_text_content(self, category_name: str, responses: Dict[str, Any], respondent_id: str) -> str:
        """Create human-readable text content for response categories with full descriptions"""
        text_parts = [f"{category_name} responses for respondent {respondent_id}:"]
        
        for field, value in responses.items():
            if value is not None:
                # Convert numpy types to Python native types
                if hasattr(value, 'item'):
                    value = value.item()
                value_str = str(value)
                
                # Find the original field name to get the description
                original_field = None
                for orig_field, desc_field in self.ontology.FIELD_NAME_MAPPING.items():
                    if desc_field == field:
                        original_field = orig_field
                        break
                
                if original_field:
                    # Get the full question description
                    #full_description = self.ontology.get_field_description(original_field)
                    #text_parts.append(f"{field} ({full_description}): {value_str}")
                    text_parts.append(f"{field}: {value_str},")
                else:
                    # Fallback to just the descriptive field name
                    text_parts.append(f"{field}: {value_str},")
        
        return " ".join(text_parts)
    
    def _calculate_data_quality_score(self, df: pd.DataFrame) -> float:
        """Calculate a simple data quality score based on completeness"""
        total_cells = df.size
        non_null_cells = df.count().sum()
        return round((non_null_cells / total_cells) * 100, 2) if total_cells > 0 else 0.0
    
    def build_knowledge_graph(self) -> Tuple[List[SurveyEntity], List[SurveyRelationship]]:
        """Main method to build the complete knowledge graph"""
        try:
            # Reset counter for consistent ID generation
            self.respondent_counter = 1
            
            # Load and validate data
            df = self.load_and_validate_data()
            
            # Store respondent ID mapping for consistent relationships
            self.respondent_id_mapping = {}
            
            # Extract different types of entities
            logger.info("Extracting respondent entities...")
            self.extract_respondent_entities(df)
            
            logger.info("Extracting response category entities...")
            self.extract_response_entities(df)
            
            logger.info("Extracting question entities...")
            self.extract_question_entities(df)
            
            logger.info("Extracting survey metadata...")
            self.extract_survey_metadata(df)
            
            # Validate relationships
            validation_result = self.validate_entity_relationships()
            if not validation_result['validation_passed']:
                logger.warning(f"Relationship validation issues found: {validation_result['orphaned_relationships'][:5]}...")
            
            logger.info(f"Built knowledge graph with {len(self.entities)} entities and {len(self.relationships)} relationships")
            
            return self.entities, self.relationships
            
        except Exception as e:
            logger.error(f"Error building knowledge graph: {e}")
            raise
    
    def validate_entity_relationships(self) -> Dict[str, Any]:
        """Validate that all relationships have valid source and target entities"""
        
        # Get all entity IDs
        entity_ids = {entity.entity_id for entity in self.entities}
        
        # Check relationships
        orphaned_relationships = []
        valid_relationships = []
        
        for rel in self.relationships:
            if rel.source_id not in entity_ids:
                orphaned_relationships.append(f"Missing source: {rel.source_id} for relationship {rel.relationship_type}")
            elif rel.target_id not in entity_ids:
                orphaned_relationships.append(f"Missing target: {rel.target_id} for relationship {rel.relationship_type}")
            else:
                valid_relationships.append(rel)
        
        return {
            'total_relationships': len(self.relationships),
            'valid_relationships': len(valid_relationships),
            'orphaned_relationships': orphaned_relationships,
            'validation_passed': len(orphaned_relationships) == 0
        }
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get comprehensive statistics about the built knowledge graph"""
        entity_counts = {}
        for entity in self.entities:
            entity_type = entity.entity_type
            entity_counts[entity_type] = entity_counts.get(entity_type, 0) + 1
        
        relationship_counts = {}
        for rel in self.relationships:
            rel_type = rel.relationship_type
            relationship_counts[rel_type] = relationship_counts.get(rel_type, 0) + 1
        
        # Calculate text content statistics
        entities_with_text = sum(1 for entity in self.entities if entity.text_content)
        
        return {
            'total_entities': len(self.entities),
            'total_relationships': len(self.relationships),
            'entity_type_counts': entity_counts,
            'relationship_type_counts': relationship_counts,
            'entities_with_text_content': entities_with_text,
            'text_content_percentage': round((entities_with_text / len(self.entities)) * 100, 2) if self.entities else 0,
            'ontology_summary': self.ontology.get_schema_summary() if hasattr(self.ontology, 'get_schema_summary') else {}
        }
    
    def export_statistics(self, output_path: Optional[str] = None) -> str:
        """Export statistics to a file"""
        stats = self.get_statistics()
        
        if output_path is None:
            output_path = Config.DATA_DIR / "kg_statistics.json"
        
        import json
        with open(output_path, 'w') as f:
            json.dump(stats, f, indent=2)
        
        logger.info(f"Statistics exported to {output_path}")
        return str(output_path)

# Example usage and testing
if __name__ == "__main__":
    # Example of how to use the KG Builder with external ontology
    print("Survey Knowledge Graph Builder with External Ontology Support")
    print("=" * 65)
    
    try:
        import argparse
        parser = argparse.ArgumentParser(description="KG Builder with External Ontology")
        parser.add_argument('--csv-file', help='Path to CSV file')
        parser.add_argument('--ontology-file', help='Path to survey_ontology.py file')
        
        args = parser.parse_args()
        
        # Initialize builder with optional external ontology
        kg_builder = SurveyKGBuilder(
            csv_file_path=args.csv_file,
            ontology_path=args.ontology_file
        )
        
        # Check if CSV file exists
        if not Path(kg_builder.csv_file_path).exists():
            print(f"CSV file not found at: {kg_builder.csv_file_path}")
            print("Please provide a valid CSV file path")
            exit(1)
        
        # Build knowledge graph
        print("Building knowledge graph...")
        entities, relationships = kg_builder.build_knowledge_graph()
        
        # Display statistics
        stats = kg_builder.get_statistics()
        print("\nKnowledge Graph Statistics:")
        print(f"Total Entities: {stats['total_entities']}")
        print(f"Total Relationships: {stats['total_relationships']}")
        print(f"Entities with Text Content: {stats['entities_with_text_content']}")
        
        print("\nEntity Type Breakdown:")
        for entity_type, count in stats['entity_type_counts'].items():
            print(f"  {entity_type}: {count}")
        
        print("\nRelationship Type Breakdown:")
        for rel_type, count in stats['relationship_type_counts'].items():
            print(f"  {rel_type}: {count}")
        
        # Export statistics
        stats_file = kg_builder.export_statistics()
        print(f"\nStatistics exported to: {stats_file}")
        
        print("\nKnowledge graph building completed successfully!")
        print("You can now proceed to the next step: Neo4j graph storage")
        
    except Exception as e:
        logger.error(f"Error in main execution: {e}")
        print(f"Error: {e}")
        exit(1)