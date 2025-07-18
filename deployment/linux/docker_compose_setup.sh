#!/bin/bash
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
