process test_query_templates {
    cache "lenient"
    memory "8 GB"
    time "2h"
    cpus "4"
    
    container "ghcr.io/ebispot/grebi_combined:dev"

    input:
    path(release_tgz)
    val(subgraphs)
    val(out_dir)
    val(export_snapshots)
    val(make_docs)
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
    path("grebi-docs.pdf"), optional: true
    path("*_snapshot_*.jsonl"), optional: true
    stdout

    script:
    """
    #!/usr/bin/env bash
    set -Eeuo pipefail
    
    echo "Extracting release tarball..."
    cat ${release_tgz} | pigz -d | tar -xf -
    cd release

    # Configure environment for the entrypoint
    export GREBI_SUBGRAPHS=${subgraphs}
    export GREBI_POSTGRES_DATA=\$PWD/postgres_data
    export SOLR_HOME=\$PWD/solr
    export SOLR_LOGS_DIR=\$PWD
    export GREBI_METADATA_JSON_SEARCH_PATH=\$PWD
    export GREBI_SQLITE_SEARCH_PATH=\$PWD
    export GREBI_QUERY_TEMPLATES_PATH=\$PWD/query_templates
    export PUBLIC_URL=/

    # Configure Neo4j for the first subgraph (entrypoint discovers others)
    FIRST_SG=\$(echo "${subgraphs}" | cut -d',' -f1)
    export NEO4J_server_directories_data=\$PWD/\${FIRST_SG}_neo4j/data
    export NEO4J_server_directories_logs=\$PWD

    # Database memory from Nextflow params
    export GREBI_NEO_HEAP=${neo_mem}
    export GREBI_SOLR_HEAP=${solr_mem}
    export GREBI_PG_SHARED_BUFFERS=${pg_shared_buffers}
    export GREBI_PG_WORK_MEM=${pg_work_mem}
    export GREBI_PG_MAINTENANCE_WORK_MEM=${pg_maintenance_work_mem}
    export GREBI_PG_MAX_WAL_SIZE=${pg_max_wal_size}

    # Snapshot export/comparison (only when requested)
    if [ "${export_snapshots}" = "true" ]; then
        export GREBI_EXPORT_SNAPSHOTS=true
        EXPECTED_DIR="${grebi_home}/tests/expected_output"
        if [ -d "\$EXPECTED_DIR" ]; then
            export GREBI_EXPECTED_DIR="\$EXPECTED_DIR"
        fi
    fi

    # Documentation generation (only when requested)
    if [ "${make_docs}" = "true" ]; then
        export GREBI_MAKE_DOCS=true
    fi

    # Run the entrypoint in test mode
    set +e
    /opt/entrypoint.sh test 2>&1 | tee ../integration_test_results.txt
    EXIT_CODE=\${PIPESTATUS[0]}
    set -e

    # Copy snapshot files to parent dir for Nextflow publishDir
    cp -f *_snapshot_*.jsonl ../ 2>/dev/null || true
    cp -f grebi-docs.pdf ../ 2>/dev/null || true

    exit \$EXIT_CODE
    """
}
