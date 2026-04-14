process link {
    cache "lenient"
    memory "4 GB"
    time "8h"

    input:
    tuple val(subgraph), val(shard_id), path(merged_filename), path(entity_metadata_jsonl), path(index_graph_metadata_json), val(exclude), val(exclude_self_referential), path(groups_txt)

    output:
    tuple val(subgraph), val(shard_id), path("linked_nodes_${subgraph}_${shard_id}.jsonl"), emit: nodes
    tuple val(subgraph), val(shard_id), path("linked_edges_${subgraph}_${shard_id}.jsonl"), emit: edges
    tuple val(subgraph), val(shard_id), path("linked_graph_metadata_${subgraph}_${shard_id}.json"), emit: linked_summary

    script:
    """
    #!/usr/bin/env bash
    set -Eeuo pipefail
    cat ${merged_filename} \
        | grebi_link \
          --in-metadata-jsonl ${entity_metadata_jsonl} \
          --in-graph-metadata-json ${index_graph_metadata_json} \
          --groups-txt ${groups_txt} \
          --out-edges-jsonl linked_edges_${subgraph}_${shard_id}.jsonl \
          --out-graph-metadata-json linked_graph_metadata_${subgraph}_${shard_id}.json \
          --exclude ${exclude.iterator().join(",")} \
          --exclude-self-referential ${exclude_self_referential.iterator().join(",")} \
        > linked_nodes_${subgraph}_${shard_id}.jsonl
    """
}
