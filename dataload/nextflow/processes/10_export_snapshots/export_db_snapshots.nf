process export_db_snapshots {
    cache "lenient"
    memory "8 GB"
    time "2h"
    cpus "4"

    container "ghcr.io/ebispot/grebi_combined:dev"

    input:
    path(neo_tgz)
    path(solr_tgz)
    val(subgraph)
    val(out_dir)
    val(grebi_home)

    publishDir "${out_dir}", overwrite: true

    output:
    path("${subgraph}_snapshot_neo4j_nodes.jsonl")
    path("${subgraph}_snapshot_neo4j_edges.jsonl")
    path("${subgraph}_snapshot_solr_nodes.jsonl")
    path("${subgraph}_snapshot_solr_edges.jsonl")

    script:
    """
    #!/usr/bin/env bash
    set -Eeuo pipefail

    echo "=== Exporting DB snapshots for ${subgraph} ==="

    # --- Extract and start Neo4j ---
    echo "Extracting Neo4j..."
    cat ${neo_tgz} | pigz -d | tar -xf -

    export NEO4J_server_directories_data=\$PWD/${subgraph}_neo4j/data
    export NEO4J_server_directories_logs=\$PWD
    export NEO4J_server_directories_run=\$PWD/neo4j_run
    export NEO4J_AUTH=none
    export NEO4J_db_recovery_fail_on_missing_files=false
    mkdir -p \$PWD/neo4j_run

    NEO4J_DIR=\$(ls -d *_neo4j 2>/dev/null | head -1)
    \$NEO4J_DIR/bin/neo4j start
    echo "Waiting for Neo4j to start..."
    for i in \$(seq 1 90); do
        if curl -sf http://127.0.0.1:7474 >/dev/null 2>&1; then
            echo "Neo4j is ready"
            break
        fi
        sleep 2
    done

    python3 ${grebi_home}/dataload/10_export_snapshots/export_neo4j.py ${subgraph}

    \$NEO4J_DIR/bin/neo4j stop || true
    sleep 5

    # --- Extract and start Solr ---
    echo "Extracting Solr..."
    cat ${solr_tgz} | pigz -d | tar -xf -
    export SOLR_HOME=\$PWD/solr
    export SOLR_LOGS_DIR=\$PWD

    /opt/solr/bin/solr start -p 8983 -force
    echo "Waiting for Solr to start..."
    for i in \$(seq 1 60); do
        if curl -s http://localhost:8983/solr/admin/cores?action=STATUS >/dev/null 2>&1; then
            echo "Solr is ready"
            break
        fi
        sleep 2
    done

    python3 ${grebi_home}/dataload/10_export_snapshots/export_solr.py ${subgraph}

    /opt/solr/bin/solr stop -p 8983 || true

    echo "=== Export complete ==="
    echo "Neo4j nodes: \$(wc -l < ${subgraph}_snapshot_neo4j_nodes.jsonl) lines"
    echo "Neo4j edges: \$(wc -l < ${subgraph}_snapshot_neo4j_edges.jsonl) lines"
    echo "Solr nodes:  \$(wc -l < ${subgraph}_snapshot_solr_nodes.jsonl) lines"
    echo "Solr edges:  \$(wc -l < ${subgraph}_snapshot_solr_edges.jsonl) lines"
    """
}
