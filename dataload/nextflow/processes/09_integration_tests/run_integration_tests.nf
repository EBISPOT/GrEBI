process run_integration_tests {
    cache "lenient"
    memory "8 GB"
    time "2h"
    cpus "4"
    
    container "ghcr.io/ebispot/grebi_combined:dev"

    input:
    path(neo_tgz)
    path(solr_tgz)
    path(sqlite)
    path(metadata_json)
    path(query_templates)
    val(subgraph)
    val(out_dir)

    publishDir "${out_dir}", overwrite: true

    output:
    path("integration_test_results.txt"), optional: true
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
    cat ${solr_tgz} | pigz -d | tar -xf - 

    export NEO4J_server_directories_data=\$PWD/${subgraph}_neo4j/data
    export NEO4J_server_directories_logs=\$PWD
    export SOLR_HOME=\$PWD/solr
    export SOLR_LOGS_DIR=\$PWD
    export GREBI_METADATA_JSON_SEARCH_PATH=\$PWD
    export GREBI_SQLITE_SEARCH_PATH=\$PWD
    export GREBI_QUERY_TEMPLATES_PATH=\$PWD/${query_templates}
    export PUBLIC_URL=/
    
    echo "Running integration tests..."
    /opt/entrypoint.sh test 2>&1 | tee integration_test_results.txt
    
    # Capture the exit code from PIPESTATUS (bash) to ensure we fail if tests fail
    TEST_EXIT_CODE=\${PIPESTATUS[0]}
    echo "Integration tests exited with code: \$TEST_EXIT_CODE"
    exit \$TEST_EXIT_CODE
    """
}
