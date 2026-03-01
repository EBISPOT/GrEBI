#!/bin/bash
set -e

echo "GrEBI Combined Stack Entrypoint"
echo "================================"

MODE="${1:-run}"

case "$MODE" in
    test)
        echo "Running in TEST mode - will run tests and exit"
        echo ""
        
        # Start all services in background
        # Redirect supervisord output to file so it doesn't keep the tee pipe open
        echo "Starting services with supervisord..."
        /usr/bin/supervisord -c /etc/supervisor/conf.d/supervisord.conf > supervisord_output.log 2>&1 &
        SUPERVISOR_PID=$!
        
        # Give supervisord a moment to start and check if it's running
        sleep 2
        if ! kill -0 $SUPERVISOR_PID 2>/dev/null; then
            echo "ERROR: supervisord failed to start"
            echo "Check supervisord.log for details"
            cat supervisord.log 2>/dev/null || echo "No supervisord.log found"
            exit 1
        fi
        
        echo "Supervisord started (PID: $SUPERVISOR_PID)"
        echo ""
        
        # Wait for services to be ready and run the tests
        # The integration_tests.py script handles the waiting with timeouts
        python3 /opt/integration_tests.py --api-url http://localhost:8090
        
        # Get the test exit code
        TEST_EXIT_CODE=$?
        
        # Stop supervisor and all child processes
        echo ""
        echo "Stopping services..."
        
        # First, use supervisorctl to stop all managed services cleanly
        supervisorctl stop all 2>/dev/null || true
        
        # Kill supervisord itself
        kill $SUPERVISOR_PID 2>/dev/null || true
        
        # Don't wait - just kill everything aggressively
        sleep 1
        
        # Kill all remaining processes by name
        killall -9 java neo4j solr caddy python3 postgres 2>/dev/null || true
        
        # Kill any remaining child processes
        pkill -9 -P $$ 2>/dev/null || true
        pkill -9 -P $SUPERVISOR_PID 2>/dev/null || true
        
        echo "Tests completed with exit code: $TEST_EXIT_CODE"
        
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
        echo "  PostgreSQL:         localhost:5432"
        echo "  GrEBI API:          http://localhost:8090"
        echo "  GrEBI UI:           http://localhost:8080"
        echo "  Metadata Service:   http://localhost:8081"
        echo "  Prefix Service:     http://localhost:8082"
        echo "  Resolver Service:   http://localhost:8084"
        echo ""
        echo "To run integration tests manually, execute:"
        echo "  python3 /opt/integration_tests.py --wait"
        echo ""
        echo "Logs are available in the current directory"
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
