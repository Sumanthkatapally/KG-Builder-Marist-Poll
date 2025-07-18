#neo4j_graph_builder.py
import logging
from typing import List, Dict, Any, Optional
from neo4j import GraphDatabase, Session
from neo4j.exceptions import ServiceUnavailable, AuthError
import json
import pandas as pd  # Add this import
from tqdm import tqdm
from kg_builder import SurveyEntity, SurveyRelationship
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

class Neo4jSurveyGraphBuilder:
    """Enhanced Neo4j database integration for survey knowledge graph with external ontology support"""
    
    def __init__(self, uri: Optional[str] = None, username: Optional[str] = None, 
                 password: Optional[str] = None, database: Optional[str] = None):
        self.uri = uri or Config.NEO4J_URI
        self.username = username or Config.NEO4J_USERNAME  
        self.password = password or Config.NEO4J_PASSWORD
        self.database = database or Config.NEO4J_DATABASE
        self.driver = None
        
    def connect(self):
        """Establish connection to Neo4j database with error handling"""
        try:
            self.driver = GraphDatabase.driver(
                self.uri, 
                auth=(self.username, self.password)
            )
            # Test connection
            with self.driver.session(database=self.database) as session:
                result = session.run("RETURN 1 as test")
                test_value = result.single()["test"]
                if test_value == 1:
                    logger.info("Successfully connected to Neo4j")
                else:
                    raise Exception("Connection test failed")
                    
        except AuthError as e:
            logger.error(f"Authentication failed: {e}")
            raise
        except ServiceUnavailable as e:
            logger.error(f"Neo4j service unavailable: {e}")
            raise
        except Exception as e:
            logger.error(f"Failed to connect to Neo4j: {e}")
            raise
    
    def close(self):
        """Close Neo4j connection"""
        if self.driver:
            self.driver.close()
            logger.info("Neo4j connection closed")
    
    def clear_database(self, confirm: bool = False):
        """Clear all survey data from database (requires confirmation)"""
        if not confirm:
            logger.warning("Database clear operation requires confirmation. Set confirm=True")
            return
            
        with self.driver.session(database=self.database) as session:
            # Delete in batches to avoid memory issues
            batch_size = 10000
            
            # First, delete all relationships
            while True:
                result = session.run(f"MATCH ()-[r]->() WITH r LIMIT {batch_size} DELETE r RETURN count(r) as deleted")
                deleted = result.single()["deleted"]
                if deleted == 0:
                    break
                logger.info(f"Deleted {deleted} relationships")
            
            # Then delete all nodes
            while True:
                result = session.run(f"MATCH (n) WITH n LIMIT {batch_size} DELETE n RETURN count(n) as deleted")
                deleted = result.single()["deleted"]
                if deleted == 0:
                    break
                logger.info(f"Deleted {deleted} nodes")
            
            logger.info("Database cleared successfully")
    
    def create_constraints_and_indexes(self):
        """Create necessary constraints and indexes for survey data"""
        constraints_and_indexes = [
            # Unique constraints
            "CREATE CONSTRAINT respondent_unique_id IF NOT EXISTS FOR (r:Respondent) REQUIRE r.unique_id IS UNIQUE",
            "CREATE CONSTRAINT survey_project_name IF NOT EXISTS FOR (s:Survey) REQUIRE s.project_name IS UNIQUE",
            
            # Indexes for performance - more generic to work with any ontology
            "CREATE INDEX respondent_survey_project IF NOT EXISTS FOR (r:Respondent) ON (r.survey_project)",
            "CREATE INDEX respondent_original_uid IF NOT EXISTS FOR (r:Respondent) ON (r.original_uid)",
            "CREATE INDEX respondent_entity_id IF NOT EXISTS FOR (r:Respondent) ON (r.entity_id)",
        ]
        
        with self.driver.session(database=self.database) as session:
            for constraint_index in constraints_and_indexes:
                try:
                    session.run(constraint_index)
                    logger.debug(f"Created: {constraint_index.split('IF NOT EXISTS')[0].strip()}")
                except Exception as e:
                    logger.warning(f"Constraint/Index creation failed (may already exist): {e}")
    
    def create_dynamic_indexes(self, entities: List[SurveyEntity]):
        """Create dynamic indexes based on the actual entity types in the data"""
        
        # Get unique entity types
        entity_types = set(entity.entity_type for entity in entities)
        
        dynamic_indexes = []
        
        for entity_type in entity_types:
            if entity_type != 'Respondent':  # Already handled in basic constraints
                # Create index on entity_id for each entity type
                dynamic_indexes.append(f"CREATE INDEX {entity_type.lower()}_entity_id IF NOT EXISTS FOR (n:{entity_type}) ON (n.entity_id)")
                
                # Create index on respondent_uid if the property exists
                sample_entities = [e for e in entities if e.entity_type == entity_type][:5]
                for entity in sample_entities:
                    if 'respondent_uid' in entity.properties:
                        dynamic_indexes.append(f"CREATE INDEX {entity_type.lower()}_respondent_uid IF NOT EXISTS FOR (n:{entity_type}) ON (n.respondent_uid)")
                        break
        
        with self.driver.session(database=self.database) as session:
            for index_query in dynamic_indexes:
                try:
                    session.run(index_query)
                    logger.debug(f"Created dynamic index: {index_query}")
                except Exception as e:
                    logger.warning(f"Dynamic index creation failed (may already exist): {e}")
    
    def create_entities_batch(self, entities: List[SurveyEntity], batch_size: Optional[int] = None):
        """Create entities in Neo4j with batch processing and progress tracking"""
        
        if batch_size is None:
            batch_size = Config.BATCH_SIZE
        
        def create_entity_batch(session: Session, entity_batch: List[SurveyEntity]):
            # Group entities by type for more efficient processing
            entities_by_type = {}
            for entity in entity_batch:
                if entity.entity_type not in entities_by_type:
                    entities_by_type[entity.entity_type] = []
                entities_by_type[entity.entity_type].append(entity)
            
            # Create entities by type
            for entity_type, type_entities in entities_by_type.items():
                entity_data = []
                for entity in type_entities:
                    # Prepare entity data with proper None handling
                    entity_props = {}
                    for key, value in entity.properties.items():
                        if value is not None:
                            entity_props[key] = value
                    
                    entity_data.append({
                        'entity_id': entity.entity_id,
                        'properties': entity_props,
                        'text_content': entity.text_content
                    })
                
                cypher = f"""
                UNWIND $entities AS entity
                CREATE (n:{entity_type})
                SET n.entity_id = entity.entity_id
                SET n += entity.properties
                SET n.text_content = entity.text_content
                RETURN count(n) as created_count
                """
                
                result = session.run(cypher, entities=entity_data)
                created_count = result.single()["created_count"]
                logger.debug(f"Created {created_count} {entity_type} entities")
        
        # Process entities in batches with progress bar
        logger.info(f"Creating {len(entities)} entities in batches of {batch_size}")
        
        with self.driver.session(database=self.database) as session:
            with tqdm(total=len(entities), desc="Creating entities") as pbar:
                for i in range(0, len(entities), batch_size):
                    batch = entities[i:i + batch_size]
                    create_entity_batch(session, batch)
                    pbar.update(len(batch))
                    logger.info(f"Created entity batch {i//batch_size + 1} ({len(batch)} entities)")
    
    def create_relationships_batch(self, relationships: List[SurveyRelationship], batch_size: Optional[int] = None):
        """Create relationships in Neo4j with batch processing and validation"""
        
        if batch_size is None:
            batch_size = Config.RELATIONSHIP_BATCH_SIZE
        
        def create_relationship_batch(session: Session, rel_batch: List[SurveyRelationship]):
            # Group relationships by type
            rels_by_type = {}
            for rel in rel_batch:
                if rel.relationship_type not in rels_by_type:
                    rels_by_type[rel.relationship_type] = []
                rels_by_type[rel.relationship_type].append(rel)
            
            # Create relationships by type with existence validation
            for rel_type, type_rels in rels_by_type.items():
                rel_data = []
                for rel in type_rels:
                    rel_props = {}
                    if rel.properties:
                        for key, value in rel.properties.items():
                            if value is not None:
                                rel_props[key] = value
                    
                    rel_data.append({
                        'source_id': rel.source_id,
                        'target_id': rel.target_id,
                        'properties': rel_props
                    })
                
                # Enhanced Cypher with validation and error handling
                cypher = f"""
                UNWIND $relationships AS rel
                MATCH (source {{entity_id: rel.source_id}})
                MATCH (target {{entity_id: rel.target_id}})
                CREATE (source)-[r:{rel_type}]->(target)
                SET r += rel.properties
                RETURN count(r) as created_count
                """
                
                try:
                    result = session.run(cypher, relationships=rel_data)
                    created_count = result.single()["created_count"]
                    
                    if created_count != len(rel_data):
                        logger.warning(f"Expected to create {len(rel_data)} {rel_type} relationships, but created {created_count}")
                    else:
                        logger.debug(f"Created {created_count} {rel_type} relationships")
                        
                except Exception as e:
                    logger.error(f"Error creating {rel_type} relationships: {e}")
                    # Try to identify problematic relationships
                    for rel in type_rels:
                        try:
                            check_cypher = """
                            MATCH (source {entity_id: $source_id})
                            MATCH (target {entity_id: $target_id})
                            RETURN count(*) as found
                            """
                            result = session.run(check_cypher, source_id=rel.source_id, target_id=rel.target_id)
                            found = result.single()["found"]
                            if found == 0:
                                logger.error(f"Missing nodes for relationship: {rel.source_id} -> {rel.target_id}")
                        except Exception as check_e:
                            logger.error(f"Error checking relationship {rel.source_id} -> {rel.target_id}: {check_e}")
        
        # Process relationships in batches with progress bar
        logger.info(f"Creating {len(relationships)} relationships in batches of {batch_size}")
        
        with self.driver.session(database=self.database) as session:
            with tqdm(total=len(relationships), desc="Creating relationships") as pbar:
                for i in range(0, len(relationships), batch_size):
                    batch = relationships[i:i + batch_size]
                    create_relationship_batch(session, batch)
                    pbar.update(len(batch))
                    logger.info(f"Created relationship batch {i//batch_size + 1} ({len(batch)} relationships)")
    
    def build_graph_from_entities(self, entities: List[SurveyEntity], relationships: List[SurveyRelationship]):
        """Build complete graph in Neo4j from entities and relationships"""
        try:
            logger.info("Starting Neo4j graph construction...")
            
            logger.info("Creating basic constraints and indexes...")
            self.create_constraints_and_indexes()
            
            logger.info("Creating dynamic indexes based on entity types...")
            self.create_dynamic_indexes(entities)
            
            logger.info(f"Creating {len(entities)} entities...")
            self.create_entities_batch(entities)
            
            logger.info(f"Creating {len(relationships)} relationships...")
            self.create_relationships_batch(relationships)
            
            # Verify graph creation
            stats = self.get_graph_statistics()
            logger.info(f"Graph creation completed. Total nodes: {stats['total_nodes']}, Total relationships: {stats['total_relationships']}")
            
        except Exception as e:
            logger.error(f"Error building graph: {e}")
            raise
    
    def get_graph_statistics(self) -> Dict[str, Any]:
        """Get comprehensive statistics about the graph in Neo4j"""
        with self.driver.session(database=self.database) as session:
            # Node counts by label
            node_counts = {}
            result = session.run("CALL db.labels() YIELD label RETURN label")
            labels = [record["label"] for record in result]
            
            for label in labels:
                count_result = session.run(f"MATCH (n:{label}) RETURN count(n) as count")
                node_counts[label] = count_result.single()["count"]
            
            # Relationship counts by type
            rel_counts = {}
            result = session.run("CALL db.relationshipTypes() YIELD relationshipType RETURN relationshipType")
            rel_types = [record["relationshipType"] for record in result]
            
            for rel_type in rel_types:
                count_result = session.run(f"MATCH ()-[r:{rel_type}]->() RETURN count(r) as count")
                rel_counts[rel_type] = count_result.single()["count"]
            
            # Additional statistics
            total_nodes = sum(node_counts.values())
            total_relationships = sum(rel_counts.values())
            
            # Data quality metrics
            quality_metrics = self._get_data_quality_metrics(session)
            
            return {
                'node_counts': node_counts,
                'relationship_counts': rel_counts,
                'total_nodes': total_nodes,
                'total_relationships': total_relationships,
                'data_quality': quality_metrics
            }
    
    def _get_data_quality_metrics(self, session: Session) -> Dict[str, Any]:
        """Calculate data quality metrics"""
        metrics = {}
        
        try:
            # Nodes with text content
            result = session.run("MATCH (n) WHERE n.text_content IS NOT NULL RETURN count(n) as count")
            metrics['nodes_with_text_content'] = result.single()["count"]
            
            # Orphaned nodes (nodes without relationships)
            result = session.run("MATCH (n) WHERE NOT (n)--() RETURN count(n) as count")
            metrics['orphaned_nodes'] = result.single()["count"]
            
            # Respondent coverage (generic approach)
            result = session.run("""
                MATCH (r:Respondent)
                OPTIONAL MATCH (r)-[]->(other)
                WHERE other:Respondent = false
                RETURN 
                    count(DISTINCT r) as total_respondents,
                    count(DISTINCT other) as connected_entities
            """)
            coverage = result.single()
            metrics['respondent_coverage'] = {
                'total': coverage["total_respondents"],
                'connected_entities': coverage["connected_entities"]
            }
            
        except Exception as e:
            logger.warning(f"Error calculating data quality metrics: {e}")
            metrics['error'] = str(e)
        
        return metrics
    
    def test_sample_queries(self) -> Dict[str, Any]:
        """Run sample queries to test the graph structure"""
        sample_results = {}
        
        with self.driver.session(database=self.database) as session:
            try:
                # Sample respondents
                result = session.run("MATCH (r:Respondent) RETURN r.unique_id as uid LIMIT 5")
                sample_results['sample_respondents'] = [record["uid"] for record in result]
                
                # Get all node types
                result = session.run("CALL db.labels() YIELD label RETURN label ORDER BY label")
                sample_results['node_types'] = [record["label"] for record in result]
                
                # Get all relationship types
                result = session.run("CALL db.relationshipTypes() YIELD relationshipType RETURN relationshipType ORDER BY relationshipType")
                sample_results['relationship_types'] = [record["relationshipType"] for record in result]
                
                # Sample relationships per respondent
                result = session.run("""
                    MATCH (r:Respondent)-[rel]->(other)
                    RETURN r.unique_id as respondent, type(rel) as relationship_type, labels(other)[0] as target_type, count(*) as count
                    ORDER BY count DESC
                    LIMIT 10
                """)
                sample_results['sample_respondent_relationships'] = [
                    {
                        'respondent': record['respondent'], 
                        'relationship_type': record['relationship_type'],
                        'target_type': record['target_type'],
                        'count': record['count']
                    } 
                    for record in result
                ]
                
                # Graph connectivity
                result = session.run("""
                    MATCH (n)
                    OPTIONAL MATCH (n)-[r]-()
                    RETURN labels(n)[0] as node_type, count(DISTINCT n) as node_count, count(r) as relationship_count
                    ORDER BY node_count DESC
                """)
                sample_results['connectivity_summary'] = [
                    {
                        'node_type': record['node_type'],
                        'node_count': record['node_count'],
                        'relationship_count': record['relationship_count']
                    }
                    for record in result
                ]
                
            except Exception as e:
                sample_results['error'] = str(e)
                logger.error(f"Error running sample queries: {e}")
        
        return sample_results
    
    def export_graph_statistics(self, output_path: Optional[str] = None) -> str:
        """Export graph statistics to a JSON file"""
        stats = self.get_graph_statistics()
        sample_queries = self.test_sample_queries()
        
        export_data = {
            'statistics': stats,
            'sample_queries': sample_queries,
            'timestamp': pd.Timestamp.now().isoformat()
        }
        
        if output_path is None:
            output_path = Config.DATA_DIR / "neo4j_graph_statistics.json"
        
        with open(output_path, 'w') as f:
            json.dump(export_data, f, indent=2, default=str)
        
        logger.info(f"Graph statistics exported to {output_path}")
        return str(output_path)

# Example usage and testing
if __name__ == "__main__":
    import pandas as pd
    from kg_builder import SurveyKGBuilder
    
    print("Neo4j Survey Graph Builder with External Ontology Support")
    print("=" * 60)
    
    try:
        import argparse
        parser = argparse.ArgumentParser(description="Neo4j Builder with External Ontology")
        parser.add_argument('--csv-file', help='Path to CSV file')
        parser.add_argument('--ontology-file', help='Path to survey_ontology.py file')
        
        args = parser.parse_args()
        
        # Initialize Neo4j builder
        neo4j_builder = Neo4jSurveyGraphBuilder()
        
        # Test connection
        print("Testing Neo4j connection...")
        neo4j_builder.connect()
        
        # Load entities and relationships from KG builder with external ontology
        print("Loading entities and relationships...")
        kg_builder = SurveyKGBuilder(
            csv_file_path=args.csv_file,
            ontology_path=args.ontology_file
        )
        entities, relationships = kg_builder.build_knowledge_graph()
        
        # Clear database (with confirmation)
        print("Clearing existing data...")
        neo4j_builder.clear_database(confirm=True)
        
        # Build graph in Neo4j
        print("Building graph in Neo4j...")
        neo4j_builder.build_graph_from_entities(entities, relationships)
        
        # Get and display statistics
        stats = neo4j_builder.get_graph_statistics()
        print("\nNeo4j Graph Statistics:")
        print(f"Total Nodes: {stats['total_nodes']}")
        print(f"Total Relationships: {stats['total_relationships']}")
        
        print("\nNode Type Breakdown:")
        for node_type, count in stats['node_counts'].items():
            print(f"  {node_type}: {count}")
        
        print("\nRelationship Type Breakdown:")
        for rel_type, count in stats['relationship_counts'].items():
            print(f"  {rel_type}: {count}")
        
        # Test sample queries
        print("\nTesting sample queries...")
        sample_results = neo4j_builder.test_sample_queries()
        
        if 'sample_respondents' in sample_results:
            print(f"Sample respondents: {sample_results['sample_respondents']}")
        
        if 'node_types' in sample_results:
            print(f"Node types found: {sample_results['node_types']}")
        
        if 'relationship_types' in sample_results:
            print(f"Relationship types found: {sample_results['relationship_types']}")
        
        # Export statistics
        stats_file = neo4j_builder.export_graph_statistics()
        print(f"\nStatistics exported to: {stats_file}")
        
        print("\nNeo4j graph building completed successfully!")
        
        # Close connection
        neo4j_builder.close()
        
    except Exception as e:
        logger.error(f"Error in main execution: {e}")
        print(f"Error: {e}")
        exit(1)