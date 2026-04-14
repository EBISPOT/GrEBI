process prepare_neo {
    cache "lenient"
    memory "4 GB" 
    time "1h"

    input:
    tuple val(subgraph), val(shard_id), path(graph_metadata_json), path(nodes_jsonl), path(edges_jsonl)

    output:
    tuple val(subgraph), val(shard_id), path("neo_nodes_${subgraph}_${shard_id}.csv"), emit: nodes
    tuple val(subgraph), val(shard_id), path("neo_edges_${subgraph}_${shard_id}.csv"), emit: edges
    tuple val(subgraph), val(shard_id), path("neo_edges_ids_${subgraph}_${shard_id}.csv"), emit: id_edges

    script:
    """
    #!/usr/bin/env bash
    set -Eeuo pipefail
    grebi_make_neo_csv \
      --in-graph-metadata-jsons ${graph_metadata_json} \
      --in-nodes-jsonl ${nodes_jsonl} \
      --in-edges-jsonl ${edges_jsonl} \
      --out-nodes-csv-path neo_nodes_${subgraph}_${shard_id}.csv \
      --out-edges-csv-path neo_edges_${subgraph}_${shard_id}.csv \
      --out-id-edges-csv-path neo_edges_ids_${subgraph}_${shard_id}.csv \
      --add-prefix ${subgraph}:
    """
}
