process run_materialised_queries {
    cache "lenient"
    memory "8 GB" 
    time "48h"
    cpus "4"
    stageInMode "copy"

    input:
    tuple val(subgraph), path(neo_db), path(query_yamls_path)
    val(neo_query_mem)
    val(out_dir)

    publishDir "${out_dir}", overwrite: true

    output:
    tuple val(subgraph), path("query_results/queries.json"), emit: metadata
    tuple val(subgraph), path("query_results/*.results.jsonl"), emit: results
    tuple val(subgraph), path("query_results/*.json"), emit: metadatas

    script:
    """
    #!/usr/bin/env bash
    set -Eeuo pipefail
    export NEO4J_HOME=${neo_db}
    export GREBI_SUBGRAPH=${subgraph}
    export NEO_MEM=${neo_query_mem}
    mkdir query_results
    PYTHONUNBUFFERED=true python3 /opt/grebi_dataload/07_run_queries/run_queries.dockerpy ${query_yamls_path}
    """
}
