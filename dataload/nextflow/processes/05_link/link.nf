process link {
    cache "lenient"
    memory "4 GB"
    time "8h"

    input:
    tuple val(subgraph), path(merged_filename), path(entity_metadata_jsonl), path(index_graph_metadata_json), val(exclude), val(exclude_self_referential), path(groups_txt)

    output:
    tuple val(subgraph), path("linked_nodes_${task.index}.jsonl"), emit: nodes
    tuple val(subgraph), path("linked_edges_${task.index}.jsonl"), emit: edges
    tuple val(subgraph), path("linked_graph_metadata_${task.index}.json"), emit: linked_summary

    script:
    """
    #!/usr/bin/env bash
    set -Eeuo pipefail
    cat ${merged_filename} \
        | grebi_link \
          --in-metadata-jsonl ${entity_metadata_jsonl} \
          --in-graph-metadata-json ${index_graph_metadata_json} \
          --groups-txt ${groups_txt} \
          --out-edges-jsonl linked_edges_${task.index}.jsonl \
          --out-graph-metadata-json linked_graph_metadata_${task.index}.json \
          --exclude ${exclude.iterator().join(",")} \
          --exclude-self-referential ${exclude_self_referential.iterator().join(",")} \
        > linked_nodes_${task.index}.jsonl
    """
}
