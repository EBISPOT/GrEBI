process add_query_metadatas_to_graph_metadata {
    cache "lenient"
    memory "8 GB" 
    time "8h"
    cpus "4"

    input:
    path(metadata_jsons)
    path(graph_metadata_json)
    val(subgraph)
    val(out_dir)

    publishDir "${out_dir}", overwrite: true

    output:
    path("${subgraph}_metadata.json")

    script:
    """
    #!/usr/bin/env bash
    set -Eeuo pipefail
    python3 /opt/grebi_dataload/07_run_queries/add_query_metadatas_to_graph_metadata.py \
        ${graph_metadata_json} \
        ${metadata_jsons} \
        > ${subgraph}_metadata.json
    """
}
