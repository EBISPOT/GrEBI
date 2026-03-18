process run_integration_tests {
    cache "lenient"
    memory "8 GB"
    time "2h"
    cpus "4"
    
    container "ghcr.io/ebispot/grebi_combined:dev"

    input:
    path(neo_tgz)
    path(solr_tgz)
    path(postgres_tgz)
    path(sqlite)
    path(metadata_json)
    path(query_templates)
    val(subgraph)
    val(out_dir)
    val(export_snapshots)
    val(grebi_home)
    val(neo_mem)
    val(solr_mem)
    val(pg_shared_buffers)
    val(pg_work_mem)
    val(pg_maintenance_work_mem)
    val(pg_max_wal_size)

    publishDir "${out_dir}", overwrite: true

    output:
    path("integration_test_results.txt"), optional: true
    path("${subgraph}_snapshot_neo4j_nodes.jsonl"), optional: true
    path("${subgraph}_snapshot_neo4j_edges.jsonl"), optional: true
    path("${subgraph}_snapshot_solr_nodes.jsonl"), optional: true
    path("${subgraph}_snapshot_postgres_edges.jsonl"), optional: true
    stdout

    script:
    """
    #!/usr/bin/env bash
    set -Eeuo pipefail
    
    echo "Extracting Neo4j..."
    mkdir -p /opt/grebi/data/neo4j
    cat ${neo_tgz} | pigz -d | tar -xf -
    
    echo "Extracting Solr..."
    mkdir -p /opt/grebi/data/solr
    # Ensure current user is resolvable (PostgreSQL requires this)
    if ! getent passwd \$(id -u) > /dev/null 2>&1; then
        echo "grebi:x:\$(id -u):\$(id -g):PostgreSQL:/tmp:/bin/bash" >> /etc/passwd 2>/dev/null || true
    fi

    # Ensure PostgreSQL socket directory exists and is writable
    mkdir -p /var/run/postgresql 2>/dev/null || true
    chmod 777 /var/run/postgresql 2>/dev/null || true

    cat ${solr_tgz} | pigz -d | tar -xf - 

    echo "Extracting PostgreSQL..."
    cat ${postgres_tgz} | pigz -d | tar -xf -
    export GREBI_POSTGRES_DATA=\$PWD/postgres_data_${subgraph}

    export NEO4J_server_directories_data=\$PWD/${subgraph}_neo4j/data
    export NEO4J_server_directories_logs=\$PWD
    export SOLR_HOME=\$PWD/solr
    export SOLR_LOGS_DIR=\$PWD
    export GREBI_METADATA_JSON_SEARCH_PATH=\$PWD
    export GREBI_SQLITE_SEARCH_PATH=\$PWD
    export GREBI_QUERY_TEMPLATES_PATH=\$PWD/${query_templates}
    export PUBLIC_URL=/

    # Configure database memory from Nextflow params
    export GREBI_NEO_HEAP=${neo_mem}
    export GREBI_SOLR_HEAP=${solr_mem}
    export GREBI_PG_SHARED_BUFFERS=${pg_shared_buffers}
    export GREBI_PG_WORK_MEM=${pg_work_mem}
    export GREBI_PG_MAINTENANCE_WORK_MEM=${pg_maintenance_work_mem}
    export GREBI_PG_MAX_WAL_SIZE=${pg_max_wal_size}
    
    # Start all services via supervisord
    echo "Starting services with supervisord..."
    /usr/bin/supervisord -c /etc/supervisor/conf.d/supervisord.conf > supervisord_output.log 2>&1 &
    SUPERVISOR_PID=\$!
    sleep 2
    if ! kill -0 \$SUPERVISOR_PID 2>/dev/null; then
        echo "ERROR: supervisord failed to start"
        cat supervisord.log 2>/dev/null || true
        exit 1
    fi

    # Run integration tests (waits for services internally)
    echo "Running integration tests..."
    set +e
    python3 /opt/integration_tests.py --api-url http://localhost:8090 2>&1 | tee integration_test_results.txt
    TEST_EXIT_CODE=\${PIPESTATUS[0]}
    set -e
    echo "Integration tests exited with code: \$TEST_EXIT_CODE"

    # Export and compare snapshots if requested
    SNAPSHOT_EXIT_CODE=0
    API_EXIT_CODE=0

    if [ "${export_snapshots}" = "true" ]; then
        echo ""
        echo "=== Exporting DB snapshots ==="

        # Neo4j and Solr are already running via supervisord
        python3 ${grebi_home}/tests/export_neo4j.py ${subgraph}
        python3 ${grebi_home}/tests/export_solr.py ${subgraph}
        python3 ${grebi_home}/tests/export_postgres.py ${subgraph}

        # Compare DB snapshots against expected output (if it exists)
        EXPECTED_DIR="${grebi_home}/tests/expected_output/${subgraph}"
        if ls "\$EXPECTED_DIR"/${subgraph}_snapshot_*.jsonl 1>/dev/null 2>&1; then
            echo ""
            echo "=== Comparing DB snapshots ==="
            set +e
            python3 ${grebi_home}/tests/compare_snapshots.py \\
                --subgraph ${subgraph} \\
                --actual-dir \$PWD \\
                --expected-dir "\$EXPECTED_DIR"
            SNAPSHOT_EXIT_CODE=\$?
            set -e
        else
            echo "No expected DB snapshots found at \$EXPECTED_DIR — skipping comparison"
            echo "To populate expected output, copy snapshot files from the pipeline output to: \$EXPECTED_DIR/"
        fi

        # Compare API snapshots (if expected snapshot exists)
        if [ -f "\$EXPECTED_DIR/${subgraph}_api_snapshot.json" ]; then
            echo ""
            echo "=== Comparing API snapshots ==="
            set +e
            python3 ${grebi_home}/tests/test_api_snapshots.py \\
                --subgraph ${subgraph} \\
                --api-url http://localhost:8090 \\
                --expected-dir "\$EXPECTED_DIR"
            API_EXIT_CODE=\$?
            set -e
        else
            echo "No expected API snapshot found — skipping API comparison"
        fi
    fi

    # Stop all services
    echo ""
    echo "Stopping services..."
    supervisorctl stop all 2>/dev/null || true
    kill \$SUPERVISOR_PID 2>/dev/null || true
    sleep 1
    killall -9 java neo4j solr caddy python3 postgres 2>/dev/null || true
    pkill -9 -P \$\$ 2>/dev/null || true
    pkill -9 -P \$SUPERVISOR_PID 2>/dev/null || true

    # Exit with combined result
    if [ \$TEST_EXIT_CODE -ne 0 ] || [ \$SNAPSHOT_EXIT_CODE -ne 0 ] || [ \$API_EXIT_CODE -ne 0 ]; then
        echo "FAILED: integration_tests=\$TEST_EXIT_CODE, db_snapshots=\$SNAPSHOT_EXIT_CODE, api_snapshots=\$API_EXIT_CODE"
        exit 1
    fi
    echo "All tests passed"
    """
}
