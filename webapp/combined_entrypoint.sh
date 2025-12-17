#!/bin/bash
set -e

echo "GrEBI Combined Stack Entrypoint"
echo "================================"

# Check for data directories and warn if empty
if [ ! -d "/opt/grebi/data/neo4j/databases" ]; then
    echo "WARNING: Neo4j data directory is empty. You need to mount Neo4j data at /opt/grebi/data/neo4j"
    echo "         The container will start but Neo4j may not have any data."
fi

if [ ! -d "/opt/grebi/data/solr/grebi" ]; then
    echo "WARNING: Solr data directory is empty. You need to mount Solr data at /opt/grebi/data/solr"
    echo "         The container will start but Solr may not have any data."
fi

# Configure Neo4j to use mounted data directory
export NEO4J_HOME=/opt/neo4j
mkdir -p /opt/grebi/data/neo4j/databases
mkdir -p /opt/grebi/data/neo4j/transactions
mkdir -p /opt/grebi/data/neo4j/logs

# Link Neo4j data directory
if [ ! -L /opt/neo4j/data ]; then
    rm -rf /opt/neo4j/data
    ln -s /opt/grebi/data/neo4j /opt/neo4j/data
fi

# Configure Solr to use mounted data directory
export SOLR_HOME=/opt/grebi/data/solr
mkdir -p /opt/grebi/data/solr

# Create metadata and sqlite directories if they don't exist
mkdir -p /opt/grebi/data/metadata
mkdir -p /opt/grebi/data/sqlite

echo ""
echo "Data directories:"
echo "  Neo4j:    /opt/grebi/data/neo4j"
echo "  Solr:     /opt/grebi/data/solr"
echo "  SQLite:   /opt/grebi/data/sqlite"
echo "  Metadata: /opt/grebi/data/metadata"
echo ""

# Determine mode
MODE="${1:-run}"

case "$MODE" in
    test)
        echo "Running in TEST mode - will run tests and exit"
        echo ""
        
        # Start all services in background
        echo "Starting services..."
        /usr/bin/supervisord -c /etc/supervisor/conf.d/supervisord.conf &
        SUPERVISOR_PID=$!
        
        # Wait for services to be ready
        echo "Waiting for services to start..."
        python3 /opt/integration_tests.py --wait --api-url http://localhost:8090
        
        # Run the tests
        TEST_EXIT_CODE=$?
        
        # Stop supervisor
        kill $SUPERVISOR_PID 2>/dev/null || true
        
        # Exit with test result
        exit $TEST_EXIT_CODE
        ;;
        
    run)
        echo "Running in CONTINUOUS mode - services will run until stopped"
        echo ""
        echo "Services will be available at:"
        echo "  Neo4j Browser:      http://localhost:7474"
        echo "  Neo4j Bolt:         bolt://localhost:7687"
        echo "  Solr Admin:         http://localhost:8983"
        echo "  GrEBI API:          http://localhost:8090"
        echo "  GrEBI UI:           http://localhost:8080"
        echo "  Resolver Service:   http://localhost:8080  (same port as UI)"
        echo "  Metadata Service:   http://localhost:8081"
        echo "  Prefix Service:     http://localhost:8082"
        echo ""
        echo "To run integration tests manually, execute:"
        echo "  python3 /opt/integration_tests.py --wait"
        echo ""
        echo "Logs are available in /var/log/supervisor/"
        echo ""
        
        # Start supervisor in foreground
        exec /usr/bin/supervisord -c /etc/supervisor/conf.d/supervisord.conf
        ;;
        
    bash)
        echo "Starting interactive bash shell"
        echo ""
        echo "Services are NOT started automatically."
        echo "To start services manually, run:"
        echo "  /usr/bin/supervisord -c /etc/supervisor/conf.d/supervisord.conf &"
        echo ""
        exec /bin/bash
        ;;
        
    *)
        echo "Unknown mode: $MODE"
        echo ""
        echo "Usage: $0 [run|test|bash]"
        echo ""
        echo "Modes:"
        echo "  run   - Start all services and run continuously (default)"
        echo "  test  - Start services, run integration tests, then exit"
        echo "  bash  - Start interactive bash shell without starting services"
        exit 1
        ;;
esac
