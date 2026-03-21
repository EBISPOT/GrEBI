#!/bin/bash
set -e

echo "GrEBI Combined Stack Entrypoint"
echo "================================"

MODE="${1:-run}"

# ---------------------------------------------------------------------------
# Service selection: if GREBI_SERVICES is set (comma-separated list), only
# start those supervisord programs.  Otherwise start everything.
#
# Neo4j / cypher_service interaction:
#   - By default (no GREBI_SERVICES): neo4j starts, cypher_service connects
#     via bolt (GREBI_NEO4J_HOSTS), no embedded path.
#   - Explicit services including neo4j: same — cypher_service uses bolt.
#   - Explicit services with cypher_service but NOT neo4j: cypher_service
#     uses embedded mode (GREBI_NEO4J_DATA_SEARCH_PATH).
# ---------------------------------------------------------------------------
ALL_SERVICES="api cypher_service neo4j metadata_service postgres prefix_service resolver_service solr ui"
SUPERVISORD_CONF="/etc/supervisor/conf.d/supervisord.conf"

# Determine which services are enabled
if [ -n "${GREBI_SERVICES:-}" ]; then
    echo "Selected services: $GREBI_SERVICES"
    IFS=',' read -ra SELECTED <<< "$GREBI_SERVICES"
else
    # All services
    IFS=' ' read -ra SELECTED <<< "$ALL_SERVICES"
fi

# Check if neo4j is in the selected list
has_neo4j=0
has_cypher=0
for sel in "${SELECTED[@]}"; do
    if [ "$sel" = "neo4j" ]; then has_neo4j=1; fi
    if [ "$sel" = "cypher_service" ]; then has_cypher=1; fi
done

# When neo4j is enabled, cypher_service must also be enabled (it proxies queries)
if [ "$has_neo4j" -eq 1 ] && [ "$has_cypher" -eq 0 ]; then
    SELECTED+=("cypher_service")
    has_cypher=1
    # Update GREBI_SERVICES so the filtering below sees it
    if [ -n "${GREBI_SERVICES:-}" ]; then
        GREBI_SERVICES="${GREBI_SERVICES},cypher_service"
    fi
fi

# Always need a filtered copy so we can tweak cypher_service env
SUPERVISORD_CONF="/tmp/supervisord_filtered.conf"
cp /etc/supervisor/conf.d/supervisord.conf "$SUPERVISORD_CONF"

# Disable services that are not in the selected list
if [ -n "${GREBI_SERVICES:-}" ]; then
    for svc in $ALL_SERVICES; do
        enabled=0
        for sel in "${SELECTED[@]}"; do
            if [ "$svc" = "$sel" ]; then enabled=1; break; fi
        done
        if [ "$enabled" -eq 0 ]; then
            sed -i "/^\[program:${svc}\]$/,/^\[/ s/^autostart=true/autostart=false/" "$SUPERVISORD_CONF"
        fi
    done
fi

# Configure cypher_service mode based on whether neo4j is running
if [ "$has_neo4j" -eq 1 ]; then
    # Bolt mode: tell cypher_service to connect to the local Neo4j server
    # Remove the embedded path and add bolt host
    sed -i '/^\[program:cypher_service\]$/,/^\[/ s/^environment=.*/environment=GREBI_NEO4J_HOSTS="bolt:\/\/localhost:7687",GREBI_CYPHER_PORT="8085"/' "$SUPERVISORD_CONF"
fi
# Otherwise cypher_service keeps its default embedded config (GREBI_NEO4J_DATA_SEARCH_PATH)

# Create logs directory so supervisord writes logs there instead of cwd
mkdir -p ./logs 2>/dev/null || true

# Ensure the current UID is resolvable in /etc/passwd (required by PostgreSQL
# and some Java tooling).  When Docker is invoked with -u UID:GID the numeric
# UID may not have a passwd entry.
CURRENT_UID=$(id -u)
CURRENT_GID=$(id -g)

if [ "$CURRENT_UID" = "0" ]; then
    # Running as root — create a dedicated grebi user for postgres
    if ! id grebi &>/dev/null; then
        groupadd -r grebi 2>/dev/null || true
        useradd -r -g grebi -d /tmp -s /bin/bash grebi 2>/dev/null || true
    fi
    GREBI_PG_USER="grebi"
else
    # Running as a non-root UID (e.g. host user via -u)
    if ! getent passwd "$CURRENT_UID" >/dev/null 2>&1; then
        echo "grebi:x:${CURRENT_UID}:${CURRENT_GID}:grebi:/tmp:/bin/bash" >> /etc/passwd 2>/dev/null || true
    fi
    if ! getent group "$CURRENT_GID" >/dev/null 2>&1; then
        echo "grebi:x:${CURRENT_GID}:" >> /etc/group 2>/dev/null || true
    fi
    # Postgres will run as the current user (supervisord user= is ignored
    # when supervisord itself is not root)
    GREBI_PG_USER=""
fi

export GREBI_PG_USER

mkdir -p /var/run/postgresql 2>/dev/null || true
chmod 777 /var/run/postgresql 2>/dev/null || true

# PostgreSQL requires the data directory to be 0700 or 0750
for pgdir in /data/postgres_data_*; do
    if [ -d "$pgdir" ]; then
        chmod 0700 "$pgdir"
    fi
done

case "$MODE" in
    test)
        echo "Running in TEST mode - will run tests and exit"
        echo ""
        
        # Start all services in background
        # Redirect supervisord output to file so it doesn't keep the tee pipe open
        echo "Starting services with supervisord..."
        /usr/bin/supervisord -c "$SUPERVISORD_CONF" > ./logs/supervisord_output.log 2>&1 &
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
        killall -9 java solr caddy python3 postgres 2>/dev/null || true
        
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
        echo "  Cypher Service:     http://localhost:8085"
        echo "  Neo4j Browser:      http://localhost:7474"
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
        exec /usr/bin/supervisord -c "$SUPERVISORD_CONF"
        ;;
        
    bash)
        echo "Starting interactive bash shell"
        echo ""
        echo "Services are NOT started automatically."
        echo "To start services manually, run:"
        echo "  /usr/bin/supervisord -c $SUPERVISORD_CONF &"
        echo ""
        exec /bin/bash
        ;;
        
    *)
        # Unknown mode — treat the arguments as a command to exec.
        # This allows Nextflow (and other tools) to run arbitrary scripts
        # inside the container while still benefiting from entrypoint setup.
        exec "$@"
        ;;
esac
