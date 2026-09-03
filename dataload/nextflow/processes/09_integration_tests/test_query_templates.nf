// Integration tests, in one of two modes:
//
//  * external Postgres (params.external_postgres, i.e. codon): test what
//    production serves — the materialised templates — against the external DB
//    populated earlier in this run, with just the API running. No Neo4j, no
//    stores, no re-extracting the multi-TB release tarball (which at codon
//    scale could not even finish inside its time limit). Live-only templates
//    are covered by the local mode.
//
//  * local (E2E / docs workflows): unpack the packaged release and boot the
//    full stack from it, proving the artifact people deploy actually works.
process test_query_templates {
    cache "lenient"
    memory "8 GB"
    time "2h"
    cpus "4"

    input:
    path(release_tgz)
    // ordering only: `none` unless the external populate ran, in which case
    // this is its status dir and the test cannot start before it finished
    val(external_done)
    val(external_postgres)
    val(subgraphs)
    val(out_dir)
    val(export_snapshots)
    val(make_docs)
    val(grebi_home)
    val(neo_mem)
    val(pg_shared_buffers)
    val(pg_work_mem)
    val(pg_maintenance_work_mem)
    val(pg_max_wal_size)

    publishDir "${out_dir}", overwrite: true

    output:
    path("integration_test_results.txt"), optional: true, emit: results
    path("grebi-docs.html"), optional: true, emit: docs_html
    path("*_snapshot_*.jsonl"), optional: true, emit: snapshots
    path("*_api_snapshot.json"), optional: true, emit: api_snapshot
    stdout emit: log

    script:
    """
    #!/usr/bin/env bash
    set -Eeuo pipefail

    export GREBI_SUBGRAPHS=${subgraphs}
    export PUBLIC_URL=/

    if [ "${external_postgres}" = "true" ]; then
        echo "Materialised-only tests against external Postgres \${PGHOST:?PGHOST must be set}"
        mkdir -p stack && cd stack
        export GREBI_SERVICES=api
        export GREBI_TEST_MATERIALISED_ONLY=true
        export GREBI_POSTGRES_HOST="\$PGHOST"
        export GREBI_POSTGRES_PORT="\${PGPORT:-5432}"
        export GREBI_POSTGRES_USER="\${PGUSER:?PGUSER must be set}"
        export GREBI_POSTGRES_DB="\${PGDATABASE:?PGDATABASE must be set}"
        export GREBI_POSTGRES_PASSWORD="\${PGPASSWORD:-}"
        if [ -n "\${PGSSLMODE:-}" ]; then export GREBI_POSTGRES_SSLMODE="\$PGSSLMODE"; fi
        export GREBI_QUERY_TEMPLATES_PATH=${grebi_home}/query_templates
    else
        echo "Extracting release tarball..."
        cat ${release_tgz} | xz -d -T0 | tar -xf -
        cd release
        export GREBI_POSTGRES_DATA=\$PWD/postgres_data
        export GREBI_SQLITE_SEARCH_PATH=\$PWD
        export GREBI_QUERY_TEMPLATES_PATH=\$PWD/query_templates

        # Configure Neo4j for the first subgraph (entrypoint discovers others)
        FIRST_SG=\$(echo "${subgraphs}" | cut -d',' -f1)
        export NEO4J_server_directories_data=\$PWD/\${FIRST_SG}_neo4j/data
        export NEO4J_server_directories_logs=\$PWD

        # Database memory from Nextflow params
        export GREBI_NEO_HEAP=${neo_mem}
        export GREBI_PG_SHARED_BUFFERS=${pg_shared_buffers}
        export GREBI_PG_WORK_MEM=${pg_work_mem}
        export GREBI_PG_MAINTENANCE_WORK_MEM=${pg_maintenance_work_mem}
        export GREBI_PG_MAX_WAL_SIZE=${pg_max_wal_size}
    fi

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
    cp -f *_api_snapshot.json ../ 2>/dev/null || true
    cp -f grebi-docs.html ../ 2>/dev/null || true

    exit \$EXIT_CODE
    """
}
