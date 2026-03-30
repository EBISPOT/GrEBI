process create_neo {
    cache "lenient"
    memory "4 GB" 
    time "48h"
    cpus "4"

    input:
    tuple val(subgraph), path(neo_inputs)
    val(neo_mem)

    output:
    tuple val(subgraph), path("${subgraph}_neo4j")

    script:
    """
    #!/usr/bin/env bash
    set -Eeuo pipefail
    cp -r /opt/neo4j ${subgraph}_neo4j
    export NEO4J_HOME=\$(pwd)/${subgraph}_neo4j
    export NEO4J_db_recovery_fail_on_missing_files=false
    bash /opt/grebi_dataload/06_create_neo_db/neo4j_import.sh ${neo_mem}
    """
}
