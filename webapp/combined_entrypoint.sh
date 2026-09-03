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
ALL_SERVICES="api cypher_service neo4j postgres ui"
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

# Copy to a writable location so sed -i works even as non-root
SUPERVISORD_CONF="/tmp/supervisord.conf"
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

# Point the API at an external Postgres when one is given. The supervisord
# block hardcodes localhost, and supervisord's environment= overrides the
# inherited env, so it has to be rewritten rather than just exported.
if [ -n "${GREBI_POSTGRES_HOST:-}" ]; then
    api_env="GREBI_CYPHER_HOST=\"http://localhost:8085\""
    api_env="$api_env,GREBI_POSTGRES_HOST=\"$GREBI_POSTGRES_HOST\""
    api_env="$api_env,GREBI_POSTGRES_PORT=\"${GREBI_POSTGRES_PORT:-5432}\""
    api_env="$api_env,GREBI_POSTGRES_USER=\"${GREBI_POSTGRES_USER:-grebi}\""
    api_env="$api_env,GREBI_POSTGRES_DB=\"${GREBI_POSTGRES_DB:-grebi}\""
    if [ -n "${GREBI_POSTGRES_PASSWORD:-}" ]; then
        api_env="$api_env,GREBI_POSTGRES_PASSWORD=\"$GREBI_POSTGRES_PASSWORD\""
    fi
    if [ -n "${GREBI_POSTGRES_SSLMODE:-}" ]; then
        api_env="$api_env,GREBI_POSTGRES_SSLMODE=\"$GREBI_POSTGRES_SSLMODE\""
    fi
    sed -i "/^\[program:api\]$/,/^\[/ s|^environment=.*|environment=$api_env|" "$SUPERVISORD_CONF"
    echo "API configured for external Postgres at $GREBI_POSTGRES_HOST"
fi

# Configure cypher_service mode based on whether neo4j is running
# Neo4j Community only supports a single database, so if there are multiple
# neo4j directories we use embedded mode (cypher_service opens each one).
NEO4J_DIR_COUNT=$(ls -d *_neo4j 2>/dev/null | wc -l)

if [ "$has_neo4j" -eq 1 ] && [ "$NEO4J_DIR_COUNT" -le 1 ]; then
    # Single graph: bolt mode — cypher_service connects to the local Neo4j server
    sed -i '/^\[program:cypher_service\]$/,/^\[/ s/^environment=.*/environment=GREBI_NEO4J_HOSTS="bolt:\/\/localhost:7687",GREBI_CYPHER_PORT="8085"/' "$SUPERVISORD_CONF"
elif [ "$NEO4J_DIR_COUNT" -gt 1 ]; then
    # Multi-graph: disable neo4j server, use embedded mode in cypher_service
    echo "Multiple Neo4j databases detected ($NEO4J_DIR_COUNT), using embedded mode"
    sed -i "/^\[program:neo4j\]$/,/^\[/ s/^autostart=true/autostart=false/" "$SUPERVISORD_CONF"
    export GREBI_NEO4J_EMBEDDED=true
fi
# Otherwise cypher_service keeps its default embedded config (GREBI_NEO4J_DATA_SEARCH_PATH)

# Inject GREBI_NEO_HEAP into the supervisord neo4j environment so the neo4j
# command (which works on a /tmp copy of the conf) can apply memory settings.
# We must NOT modify *_neo4j/conf/neo4j.conf directly because it may be a
# bind-mount from the host.
if [ -n "${GREBI_NEO_HEAP:-}" ]; then
    echo "Configuring Neo4j memory: heap=$GREBI_NEO_HEAP, pagecache=$GREBI_NEO_HEAP"
    sed -i "/^\[program:neo4j\]$/,/^\[/ s|^environment=.*|environment=NEO4J_AUTH=\"none\",GREBI_NEO_HEAP=\"$GREBI_NEO_HEAP\"|" "$SUPERVISORD_CONF"
fi

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
    # Postgres must run as the current (non-root) UID: supervisord is not root,
    # so it cannot setuid to a different user (it errors, it does not silently
    # ignore user=). Use the numeric UID (resolvable via the /etc/passwd entry
    # added above) rather than the name "grebi", which the image's pre-created
    # grebi user (a different, lower UID) would otherwise shadow — causing
    # "couldn't setuid to <uid>: Can't drop privilege as nonroot user".
    GREBI_PG_USER="$CURRENT_UID"
fi

export GREBI_PG_USER

mkdir -p /var/run/postgresql 2>/dev/null || true
chmod 777 /var/run/postgresql 2>/dev/null || true

# PostgreSQL requires the data directory to be 0700 or 0750
for pgdir in /data/postgres_data*; do
    if [ -d "$pgdir" ]; then
        chmod 0700 "$pgdir"
    fi
done
if [ -n "${GREBI_POSTGRES_DATA:-}" ] && [ -d "$GREBI_POSTGRES_DATA" ]; then
    chmod 0700 "$GREBI_POSTGRES_DATA"
fi

case "$MODE" in
    test)
        echo "Running in TEST mode - will run tests and exit"
        echo ""
        
        # Start all services in background
        echo "Starting services with supervisord..."
        supervisord -c "$SUPERVISORD_CONF" > ./logs/supervisord_output.log 2>&1 &
        SUPERVISOR_PID=$!
        
        # Give supervisord a moment to start and check if it's running
        sleep 2
        if ! kill -0 $SUPERVISOR_PID 2>/dev/null; then
            echo "ERROR: supervisord failed to start"
            echo "Check supervisord.log for details"
            cat ./logs/supervisord.log 2>/dev/null || echo "No supervisord.log found"
            exit 1
        fi
        
        echo "Supervisord started (PID: $SUPERVISOR_PID)"
        echo ""
        
        # ---------------------------------------------------------------
        # Phase 1: Integration tests (and optional doc generation)
        # ---------------------------------------------------------------
        MAKE_DOCS_ARG=""
        if [ "${GREBI_MAKE_DOCS:-}" = "true" ]; then
            MAKE_DOCS_ARG="--make-docs --docs-dir /opt/docs --output grebi-docs.html"
        fi
        # Materialised-only: exercise just the templates served from Postgres
        # (no Neo4j/cypher in this stack), e.g. against an external DB.
        MATERIALISED_ONLY_ARG=""
        if [ "${GREBI_TEST_MATERIALISED_ONLY:-}" = "true" ]; then
            MATERIALISED_ONLY_ARG="--materialised-only"
        fi

        set +e
        python3 -u /opt/test_queries_and_make_docs.py --api-url http://localhost:8090 $MAKE_DOCS_ARG $MATERIALISED_ONLY_ARG
        TEST_EXIT_CODE=$?
        set -e
        echo ""
        echo "Integration tests exited with code: $TEST_EXIT_CODE"
        
        # ---------------------------------------------------------------
        # Phase 2: Snapshot export (only when GREBI_EXPORT_SNAPSHOTS=true)
        # ---------------------------------------------------------------
        SNAPSHOT_EXIT_CODE=0
        API_EXIT_CODE=0
        
        if [ "${GREBI_EXPORT_SNAPSHOTS:-}" = "true" ]; then
            # Auto-detect subgraph name from *_metadata.json in working dir
            SUBGRAPH=""
            for f in *_metadata.json; do
                [ -f "$f" ] || continue
                SUBGRAPH="${f%_metadata.json}"
                break
            done
            
            if [ -n "$SUBGRAPH" ]; then
                echo ""
                echo "=== Exporting DB snapshots for '$SUBGRAPH' ==="
                set +e
                python3 /opt/export_neo4j.py "$SUBGRAPH"
                python3 /opt/export_postgres.py "$SUBGRAPH"
                set -e

                # Always generate the API snapshot into the working dir so it is
                # published to the pipeline output for initial population, exactly
                # like the DB snapshots above (see tests/expected_output/README.md).
                # The comparison against committed expected output (if any) happens
                # in Phase 3 below.
                echo ""
                echo "=== Exporting API snapshot for '$SUBGRAPH' ==="
                set +e
                python3 /opt/test_api_snapshots.py \
                    --subgraph "$SUBGRAPH" \
                    --api-url http://localhost:8090 \
                    --expected-dir "$PWD" \
                    --update
                set -e

                # Phase 3: Compare snapshots against expected output (if requested).
                # Expected snapshots are committed per-subgraph in
                # tests/expected_output/<subgraph>/ (see that dir's README), so
                # look in the subgraph subdirectory rather than the parent.
                if [ -n "${GREBI_EXPECTED_DIR:-}" ]; then
                    EXPECTED_SG_DIR="$GREBI_EXPECTED_DIR/$SUBGRAPH"
                    if ls "$EXPECTED_SG_DIR"/${SUBGRAPH}_snapshot_*.jsonl 1>/dev/null 2>&1; then
                        echo ""
                        echo "=== Comparing DB snapshots ==="
                        set +e
                        python3 /opt/compare_snapshots.py \
                            --subgraph "$SUBGRAPH" \
                            --actual-dir "$PWD" \
                            --expected-dir "$EXPECTED_SG_DIR"
                        SNAPSHOT_EXIT_CODE=$?
                        set -e
                    else
                        echo "No expected DB snapshots found at $EXPECTED_SG_DIR — skipping comparison"
                    fi

                    if [ -f "$EXPECTED_SG_DIR/${SUBGRAPH}_api_snapshot.json" ]; then
                        echo ""
                        echo "=== Comparing API snapshots ==="
                        set +e
                        python3 /opt/test_api_snapshots.py \
                            --subgraph "$SUBGRAPH" \
                            --api-url http://localhost:8090 \
                            --expected-dir "$EXPECTED_SG_DIR"
                        API_EXIT_CODE=$?
                        set -e
                    else
                        echo "No expected API snapshot found — skipping API comparison"
                    fi
                fi
            else
                echo "Warning: could not detect subgraph name (no *_metadata.json found) — skipping snapshot export"
            fi
        fi
        
        # ---------------------------------------------------------------
        # Cleanup
        # ---------------------------------------------------------------
        echo ""
        echo "Stopping services..."
        supervisorctl stop all 2>/dev/null || true
        kill $SUPERVISOR_PID 2>/dev/null || true
        sleep 1
        killall -9 java neo4j caddy python3 postgres 2>/dev/null || true
        pkill -9 -P $$ 2>/dev/null || true
        pkill -9 -P $SUPERVISOR_PID 2>/dev/null || true
        
        # Combined exit code
        if [ $TEST_EXIT_CODE -ne 0 ] || [ $SNAPSHOT_EXIT_CODE -ne 0 ] || [ $API_EXIT_CODE -ne 0 ]; then
            echo "FAILED: integration_tests=$TEST_EXIT_CODE, db_snapshots=$SNAPSHOT_EXIT_CODE, api_snapshots=$API_EXIT_CODE"
            exit 1
        fi
        echo "All tests passed"
        exit 0
        ;;
        
    run)
        echo "Running in CONTINUOUS mode - services will run until stopped"
        echo ""
        echo "Services will be available at:"
        for sel in "${SELECTED[@]}"; do
            case "$sel" in
                cypher_service)   echo "  Cypher Service:     http://localhost:8085" ;;
                neo4j)            echo "  Neo4j Browser:      http://localhost:7474" ;;
                postgres)         echo "  PostgreSQL:         localhost:5432" ;;
                api)              echo "  GrEBI API:          http://localhost:8090" ;;
                ui)               echo "  GrEBI UI:           http://localhost:8080" ;;
            esac
        done
        echo ""
        echo "To run integration tests manually, execute:"
        echo "  python3 /opt/test_query_templates.py --wait"
        echo ""
        echo "Logs are available in the current directory"
        echo ""
        
        # Start supervisor in foreground
        exec supervisord -c "$SUPERVISORD_CONF"
        ;;
        
    bash)
        if [ $# -eq 1 ]; then
            echo "Starting interactive bash shell"
            echo ""
            echo "Services are NOT started automatically."
            echo "To start services manually, run:"
            echo "  supervisord -c $SUPERVISORD_CONF &"
            echo ""
            exec /bin/bash
        else
            # bash with arguments (e.g. bash -c "...") — execute directly
            exec "$@"
        fi
        ;;
        
    *)
        # Unknown mode — treat the arguments as a command to exec.
        # This allows Nextflow (and other tools) to run arbitrary scripts
        # inside the container while still benefiting from entrypoint setup.
        exec "$@"
        ;;
esac
