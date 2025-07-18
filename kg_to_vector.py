import logging
import json
from typing import List, Dict, Any, Optional, Tuple
from neo4j import GraphDatabase
import numpy as np
from dataclasses import dataclass
from tqdm import tqdm
import pandas as pd

from model_manager import model_manager
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
class VectorizedEntity:
    """Represents an entity with its vector embedding"""
    entity_id: str
    entity_type: str
    text_content: str
    embedding: List[float]
    metadata: Dict[str, Any]

class SurveyVectorBuilder:
    """Converts survey knowledge graph to vector embeddings using local models"""
    
    def __init__(self, neo4j_uri: Optional[str] = None, neo4j_user: Optional[str] = None, 
                 neo4j_password: Optional[str] = None, database: Optional[str] = None):
        self.neo4j_driver = GraphDatabase.driver(
            neo4j_uri or Config.NEO4J_URI, 
            auth=(neo4j_user or Config.NEO4J_USERNAME, neo4j_password or Config.NEO4J_PASSWORD)
        )
        self.database = database or Config.NEO4J_DATABASE
        self.model_manager = model_manager
        
    def close(self):
        """Close connections"""
        if self.neo4j_driver:
            self.neo4j_driver.close()
    
    def initialize_models(self):
        """Initialize embedding model"""
        try:
            logger.info("Initializing embedding model...")
            self.model_manager.download_and_load_embedding_model()
            logger.info("Embedding model ready")
        except Exception as e:
            logger.error(f"Error initializing models: {e}")
            raise
    
    def extract_entity_text_content(self) -> List[Dict[str, Any]]:
        """Extract text content from graph entities for vectorization"""
        text_entities = []
        
        with self.neo4j_driver.session(database=self.database) as session:
            # Extract respondent summaries with all connected data
            respondent_query = """
            MATCH (r:Respondent)
            OPTIONAL MATCH (r)-[:HAS_DEMOGRAPHIC]->(d:Demographics)
            OPTIONAL MATCH (r)-[:HAS_POLITICAL_OPINION]->(p:PoliticalOpinions)
            OPTIONAL MATCH (r)-[:HAS_VALUES_SOCIAL_ISSUES]->(v:ValuesAndSocialIssues)
            OPTIONAL MATCH (r)-[:HAS_LIFESTYLE_BEHAVIOR]->(l:LifestyleAndBehavioralOpinions)
            OPTIONAL MATCH (r)-[:HAS_PUBLIC_POLICY]->(pp:PublicPolicyCivicEngagement)
            RETURN r.entity_id as entity_id, 'Respondent' as entity_type,
                   r{.*} as respondent_props, d{.*} as demo_props, 
                   p{.*} as political_props, v{.*} as values_props,
                   l{.*} as lifestyle_props, pp{.*} as policy_props
            """
            
            result = session.run(respondent_query)
            for record in result:
                text_content = self._create_comprehensive_respondent_summary(
                    record['respondent_props'],
                    record['demo_props'], 
                    record['political_props'],
                    record['values_props'],
                    record['lifestyle_props'],
                    record['policy_props']
                )
                
                text_entities.append({
                    'entity_id': record['entity_id'],
                    'entity_type': record['entity_type'],
                    'text_content': text_content,
                    'metadata': {
                        'respondent_props': record['respondent_props'],
                        'demo_props': record['demo_props'],
                        'political_props': record['political_props'],
                        'values_props': record['values_props'],
                        'lifestyle_props': record['lifestyle_props'],
                        'policy_props': record['policy_props']
                    }
                })
            
            # Extract response category summaries
            response_categories = [
                'Demographics', 'PoliticalOpinions', 'ValuesAndSocialIssues',
                'LifestyleAndBehavioralOpinions', 'PublicPolicyCivicEngagement',
                'TechnicalSurveyMetadata'
            ]
            
            for category in response_categories:
                category_query = f"""
                MATCH (n:{category})
                RETURN n.entity_id as entity_id, '{category}' as entity_type, 
                       n{{.*}} as properties, n.text_content as existing_text
                """
                
                result = session.run(category_query)
                for record in result:
                    # Use existing text content if available, otherwise create new
                    text_content = record['existing_text'] or self._create_category_summary(
                        category, record['properties']
                    )
                    
                    text_entities.append({
                        'entity_id': record['entity_id'],
                        'entity_type': record['entity_type'], 
                        'text_content': text_content,
                        'metadata': record['properties']
                    })
            
            # Extract question entities
            question_query = """
            MATCH (q:Question)
            RETURN q.entity_id as entity_id, 'Question' as entity_type,
                   q{.*} as properties, q.text_content as existing_text
            """
            
            result = session.run(question_query)
            for record in result:
                text_content = record['existing_text'] or self._create_question_summary(
                    record['properties']
                )
                
                text_entities.append({
                    'entity_id': record['entity_id'],
                    'entity_type': record['entity_type'],
                    'text_content': text_content,
                    'metadata': record['properties']
                })
        
        logger.info(f"Extracted {len(text_entities)} entities for vectorization")
        return text_entities
    
    def generate_embeddings(self, text_entities: List[Dict[str, Any]], 
                          batch_size: int = 32) -> List[VectorizedEntity]:
        """Generate embeddings for text entities using local model"""
        vectorized_entities = []
        
        # Process in batches for memory efficiency
        logger.info(f"Generating embeddings for {len(text_entities)} entities...")
        
        with tqdm(total=len(text_entities), desc="Generating embeddings") as pbar:
            for i in range(0, len(text_entities), batch_size):
                batch = text_entities[i:i + batch_size]
                
                # Prepare texts for embedding
                texts = [entity['text_content'] for entity in batch]
                
                try:
                    # Generate embeddings using local model
                    embeddings = self.model_manager.generate_embeddings(texts)
                    
                    # Process results
                    for j, entity in enumerate(batch):
                        embedding = embeddings[j].tolist()  # Convert numpy array to list
                        
                        vectorized_entity = VectorizedEntity(
                            entity_id=entity['entity_id'],
                            entity_type=entity['entity_type'],
                            text_content=entity['text_content'],
                            embedding=embedding,
                            metadata=entity['metadata']
                        )
                        vectorized_entities.append(vectorized_entity)
                    
                    pbar.update(len(batch))
                    
                except Exception as e:
                    logger.error(f"Error generating embeddings for batch {i//batch_size + 1}: {e}")
                    continue
        
        logger.info(f"Generated embeddings for {len(vectorized_entities)} entities")
        return vectorized_entities
    
    def store_embeddings_in_neo4j(self, vectorized_entities: List[VectorizedEntity], 
                                 batch_size: int = 100):
        """Store embeddings back in Neo4j as node properties"""
        
        logger.info(f"Storing {len(vectorized_entities)} embeddings in Neo4j...")
        
        with self.neo4j_driver.session(database=self.database) as session:
            # Store embeddings in batches
            with tqdm(total=len(vectorized_entities), desc="Storing embeddings") as pbar:
                for i in range(0, len(vectorized_entities), batch_size):
                    batch = vectorized_entities[i:i + batch_size]
                    
                    # Prepare batch data
                    batch_data = []
                    for entity in batch:
                        batch_data.append({
                            'entity_id': entity.entity_id,
                            'embedding': entity.embedding,
                            'text_content': entity.text_content,
                            'embedding_dimension': len(entity.embedding)
                        })
                    
                    # Update nodes with embeddings
                    cypher = """
                    UNWIND $batch AS item
                    MATCH (n {entity_id: item.entity_id})
                    SET n.embedding = item.embedding,
                        n.text_content = item.text_content,
                        n.embedding_dimension = item.embedding_dimension,
                        n.embedding_model = $model_name
                    RETURN count(n) as updated_count
                    """
                    
                    try:
                        result = session.run(cypher, batch=batch_data, model_name=Config.EMBEDDING_MODEL_NAME)
                        updated_count = result.single()["updated_count"]
                        
                        if updated_count != len(batch):
                            logger.warning(f"Expected to update {len(batch)} nodes, but updated {updated_count}")
                        
                        pbar.update(len(batch))
                        
                    except Exception as e:
                        logger.error(f"Error storing embeddings batch {i//batch_size + 1}: {e}")
                        continue
    
    def create_vector_index(self, index_name: Optional[str] = None):
        """Create vector index in Neo4j for similarity search"""
        
        if index_name is None:
            index_name = Config.VECTOR_INDEX_NAME
        
        with self.neo4j_driver.session(database=self.database) as session:
            # Check if index already exists
            check_query = "SHOW INDEXES YIELD name WHERE name = $index_name"
            result = session.run(check_query, index_name=index_name)
            
            if result.single():
                logger.info(f"Vector index '{index_name}' already exists")
                return
            
            # Create vector index for respondents (primary entities for search)
            create_index_query = f"""
            CREATE VECTOR INDEX {index_name} IF NOT EXISTS
            FOR (n:Respondent) ON (n.embedding)
            OPTIONS {{
                indexConfig: {{
                    `vector.dimensions`: {Config.EMBEDDING_DIMENSION},
                    `vector.similarity_function`: 'cosine'
                }}
            }}
            """
            
            try:
                session.run(create_index_query)
                logger.info(f"Created vector index '{index_name}' for Respondent nodes")
                
                # Create additional indexes for other entity types
                other_indexes = [
                    ("demographics_vector_index", "Demographics"),
                    ("political_vector_index", "PoliticalOpinions"),
                    ("values_vector_index", "ValuesAndSocialIssues")
                ]
                
                for idx_name, entity_type in other_indexes:
                    create_additional_index = f"""
                    CREATE VECTOR INDEX {idx_name} IF NOT EXISTS
                    FOR (n:{entity_type}) ON (n.embedding)
                    OPTIONS {{
                        indexConfig: {{
                            `vector.dimensions`: {Config.EMBEDDING_DIMENSION},
                            `vector.similarity_function`: 'cosine'
                        }}
                    }}
                    """
                    
                    try:
                        session.run(create_additional_index)
                        logger.info(f"Created vector index '{idx_name}' for {entity_type} nodes")
                    except Exception as e:
                        logger.warning(f"Could not create vector index for {entity_type}: {e}")
                        
            except Exception as e:
                logger.error(f"Error creating vector indexes: {e}")
                raise
    
    def test_vector_search(self, query_text: str, top_k: int = 5) -> List[Dict[str, Any]]:
        """Test vector similarity search"""
        try:
            # Generate embedding for query
            query_embedding = self.model_manager.generate_embeddings([query_text])[0].tolist()
            
            # Perform vector search
            with self.neo4j_driver.session(database=self.database) as session:
                search_query = f"""
                CALL db.index.vector.queryNodes('{Config.VECTOR_INDEX_NAME}', $top_k, $query_vector)
                YIELD node, score
                RETURN node.entity_id as entity_id, 
                       node.text_content as text_content,
                       labels(node) as labels,
                       score
                ORDER BY score DESC
                """
                
                result = session.run(search_query, top_k=top_k, query_vector=query_embedding)
                
                search_results = []
                for record in result:
                    search_results.append({
                        'entity_id': record['entity_id'],
                        'text_content': record['text_content'],
                        'labels': record['labels'],
                        'similarity_score': record['score']
                    })
                
                return search_results
                
        except Exception as e:
            logger.error(f"Error in vector search: {e}")
            return []
    
    def build_vector_graph(self) -> List[VectorizedEntity]:
        """Main method to build vector-enabled knowledge graph"""
        try:
            # Initialize models
            self.initialize_models()
            
            # Extract text content from entities
            logger.info("Extracting text content from graph entities...")
            text_entities = self.extract_entity_text_content()
            
            # Generate embeddings
            logger.info("Generating embeddings...")
            vectorized_entities = self.generate_embeddings(text_entities)
            
            # Store embeddings in Neo4j
            logger.info("Storing embeddings in Neo4j...")
            self.store_embeddings_in_neo4j(vectorized_entities)
            
            # Create vector indexes
            logger.info("Creating vector indexes...")
            self.create_vector_index()
            
            # Test vector search
            logger.info("Testing vector search...")
            test_results = self.test_vector_search("Biden approval rating")
            if test_results:
                logger.info(f"Vector search test successful: found {len(test_results)} results")
                for i, result in enumerate(test_results[:3]):
                    logger.info(f"  {i+1}. {result['entity_id']}: {result['similarity_score']:.3f}")
            
            logger.info("Vector graph building completed successfully")
            return vectorized_entities
            
        except Exception as e:
            logger.error(f"Error building vector graph: {e}")
            raise
    
    def _create_comprehensive_respondent_summary(self, respondent_props: Dict, demo_props: Dict, 
                                               political_props: Dict, values_props: Dict,
                                               lifestyle_props: Dict, policy_props: Dict) -> str:
        """Create a comprehensive text summary of a respondent for embedding"""
        summary_parts = []
        
        # Basic respondent info
        if respondent_props:
            uid = respondent_props.get('unique_id', 'unknown')
            survey_project = respondent_props.get('survey_project', 'unknown')
            summary_parts.append(f"Survey respondent {uid} from {survey_project}")
        
        # Demographics
        if demo_props:
            demo_text = self._format_demographics(demo_props)
            if demo_text:
                summary_parts.append(f"Demographics: {demo_text}")
        
        # Political opinions
        if political_props:
            political_text = self._format_political_opinions(political_props)
            if political_text:
                summary_parts.append(f"Political views: {political_text}")
        
        # Values and social issues
        if values_props:
            values_text = self._format_values_social(values_props)
            if values_text:
                summary_parts.append(f"Values and beliefs: {values_text}")
        
        # Lifestyle and behavior
        if lifestyle_props:
            lifestyle_text = self._format_lifestyle_behavioral(lifestyle_props)
            if lifestyle_text:
                summary_parts.append(f"Lifestyle: {lifestyle_text}")
        
        # Public policy and civic engagement
        if policy_props:
            policy_text = self._format_public_policy(policy_props)
            if policy_text:
                summary_parts.append(f"Civic engagement: {policy_text}")
        
        return ". ".join(summary_parts)
    
    def _create_category_summary(self, category: str, properties: Dict) -> str:
        """Create text summary for response category"""
        if not properties:
            return f"{category} response with no data"
        
        # Filter out metadata fields
        content_props = {k: v for k, v in properties.items() 
                        if k not in ['entity_id', 'respondent_uid', 'response_category', 'response_count'] 
                        and v is not None}
        
        if category == 'Demographics':
            return self._format_demographics(content_props)
        elif category == 'PoliticalOpinions':
            return self._format_political_opinions(content_props)
        elif category == 'ValuesAndSocialIssues':
            return self._format_values_social(content_props)
        elif category == 'LifestyleAndBehavioralOpinions':
            return self._format_lifestyle_behavioral(content_props)
        elif category == 'PublicPolicyCivicEngagement':
            return self._format_public_policy(content_props)
        else:
            return f"{category}: {', '.join([f'{k}={v}' for k, v in content_props.items()])}"
    
    def _create_question_summary(self, properties: Dict) -> str:
        """Create text summary for question entities"""
        field_name = properties.get('field_name', 'unknown')
        description = properties.get('description', field_name)
        category = properties.get('category', 'unknown')
        
        return f"Survey question {field_name} about {description} in {category} category"
    
    def _format_demographics(self, props: Dict) -> str:
        """Format demographic properties into readable text with descriptions"""
        demo_parts = []
        
        # Key demographic fields with full descriptions
        field_mappings = {
            'gender': 'Gender identity',
            'stateofresidence': 'State of residence', 
            'ager': 'Age group',
            'recodedraceforweighting': 'Race/ethnicity identity',
            'inc15wt': 'Household income level',
            'educationlevel': 'Education level completed',
            'urbanpopulationdensity': 'Area type where respondent lives'
        }
        
        for field, description in field_mappings.items():
            if field in props and props[field] is not None:
                demo_parts.append(f"{description}: {props[field]}")
        
        return ", ".join(demo_parts)
    
    def _format_political_opinions(self, props: Dict) -> str:
        """Format political opinion properties into readable text with descriptions"""
        political_parts = []
        
        # Key political fields with full descriptions
        field_mappings = {
            'jobapprovalbiden': 'Biden job approval rating',
            'voterpreferencein2020presidentialelection': '2020 presidential vote choice',
            'partyidforweighting': 'Political party identification',
            'economicimpactimmigration': 'View on immigration economic impact',
            'electionwinbelief': 'Belief about Biden winning 2020 election'
        }
        
        for field, description in field_mappings.items():
            if field in props and props[field] is not None:
                political_parts.append(f"{description}: {props[field]}")
        
        return ", ".join(political_parts)
    
    def _format_values_social(self, props: Dict) -> str:
        """Format values and social issues into readable text with descriptions"""
        values_parts = []
        
        # Key value fields with full descriptions
        field_mappings = {
            'attainabilityoftheamericandream': 'American Dream attainability belief',
            'valuesocialcorpgreedinflation': 'Corporate greed causes inflation belief',
            'friendshipdivergence': 'Has friends with different political views',
            'politicalcorrectnessdebate': 'America too politically correct opinion',
            'resorttoviolenceontrack': 'Americans may need violence for change belief'
        }
        
        for field, description in field_mappings.items():
            if field in props and props[field] is not None:
                values_parts.append(f"{description}: {props[field]}")
        
        return ", ".join(values_parts)
    
    def _format_lifestyle_behavioral(self, props: Dict) -> str:
        """Format lifestyle and behavioral properties with descriptions"""
        lifestyle_parts = []
        
        field_mappings = {
            'cellphonecallanswerfrequency': 'Phone usage pattern for calls',
            'lifecigarettecount': 'Smoking history (100+ cigarettes)',
            'productearlyadopter': 'Early adopter of new products',
            'attendsportseventsfrequency': 'Frequency attending sports events'
        }
        
        for field, description in field_mappings.items():
            if field in props and props[field] is not None:
                lifestyle_parts.append(f"{description}: {props[field]}")
        
        return ", ".join(lifestyle_parts)
    
    def _format_public_policy(self, props: Dict) -> str:
        """Format public policy and civic engagement properties with descriptions"""
        policy_parts = []
        
        field_mappings = {
            'rvx': 'Voter registration status',
            'publicpolicyengagementfactor': 'Survey weighting factor'
        }
        
        for field, description in field_mappings.items():
            if field in props and props[field] is not None:
                policy_parts.append(f"{description}: {props[field]}")
        
        return ", ".join(policy_parts)
    
    def export_vector_statistics(self, output_path: Optional[str] = None) -> str:
        """Export vector statistics to a JSON file"""
        try:
            with self.neo4j_driver.session(database=self.database) as session:
                # Count nodes with embeddings by type
                embedding_stats = {}
                
                node_types = ['Respondent', 'Demographics', 'PoliticalOpinions', 'ValuesAndSocialIssues', 
                             'LifestyleAndBehavioralOpinions', 'PublicPolicyCivicEngagement', 'Question']
                
                for node_type in node_types:
                    result = session.run(f"""
                        MATCH (n:{node_type})
                        RETURN 
                            count(n) as total_nodes,
                            count(n.embedding) as nodes_with_embeddings,
                            avg(size(n.embedding)) as avg_embedding_dimension
                    """)
                    
                    record = result.single()
                    if record:
                        embedding_stats[node_type] = {
                            'total_nodes': record['total_nodes'],
                            'nodes_with_embeddings': record['nodes_with_embeddings'],
                            'avg_embedding_dimension': record['avg_embedding_dimension']
                        }
                
                # Test vector search performance
                test_queries = [
                    "Biden approval rating",
                    "Democratic voter",
                    "Republican views",
                    "California resident",
                    "Conservative values"
                ]
                
                search_performance = {}
                for query in test_queries:
                    results = self.test_vector_search(query, top_k=5)
                    search_performance[query] = {
                        'results_count': len(results),
                        'top_score': results[0]['similarity_score'] if results else 0.0
                    }
                
                export_data = {
                    'embedding_statistics': embedding_stats,
                    'search_performance': search_performance,
                    'model_info': self.model_manager.get_model_info(),
                    'config': {
                        'embedding_model': Config.EMBEDDING_MODEL_NAME,
                        'embedding_dimension': Config.EMBEDDING_DIMENSION,
                        'vector_index_name': Config.VECTOR_INDEX_NAME
                    },
                    'timestamp': pd.Timestamp.now().isoformat()
                }
                
                if output_path is None:
                    output_path = Config.DATA_DIR / "vector_statistics.json"
                
                with open(output_path, 'w') as f:
                    json.dump(export_data, f, indent=2, default=str)
                
                logger.info(f"Vector statistics exported to {output_path}")
                return str(output_path)
                
        except Exception as e:
            logger.error(f"Error exporting vector statistics: {e}")
            raise

# Example usage and testing
if __name__ == "__main__":
    print("Survey Knowledge Graph Vector Builder")
    print("=" * 50)
    
    try:
        # Initialize vector builder
        vector_builder = SurveyVectorBuilder()
        
        print("Building vector-enabled knowledge graph...")
        vectorized_entities = vector_builder.build_vector_graph()
        
        print(f"\nVector embedding completed for {len(vectorized_entities)} entities")
        
        # Test vector search
        print("\nTesting vector search functionality...")
        test_queries = [
            "Biden approval rating",
            "California Democrats", 
            "Conservative Republicans",
            "Young voters"
        ]
        
        for query in test_queries:
            print(f"\nSearching for: '{query}'")
            results = vector_builder.test_vector_search(query, top_k=3)
            
            if results:
                for i, result in enumerate(results):
                    print(f"  {i+1}. {result['entity_id']}: {result['similarity_score']:.3f}")
                    print(f"     {result['text_content'][:100]}...")
            else:
                print("  No results found")
        
        # Export statistics
        stats_file = vector_builder.export_vector_statistics()
        print(f"\nVector statistics exported to: {stats_file}")
        
        print("\nVector embedding completed successfully!")
        print("You can now proceed to the next step: Cypher query generation")
        
        # Close connections
        vector_builder.close()
        
    except Exception as e:
        logger.error(f"Error in main execution: {e}")
        print(f"Error: {e}")
        exit(1)