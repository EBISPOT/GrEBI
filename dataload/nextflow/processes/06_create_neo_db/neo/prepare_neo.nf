process prepare_neo {
    cache "lenient"
    memory "4 GB" 
    time "1h"

    input:
    tuple val(subgraph), path(graph_metadata_json), path(nodes_jsonl), path(edges_jsonl)

    output:
    tuple val(subgraph), path("neo_nodes_${subgraph}_${task.index}.csv"), emit: nodes
    tuple val(subgraph), path("neo_edges_${subgraph}_${task.index}.csv"), emit: edges
    tuple val(subgraph), path("neo_edges_ids_${subgraph}_${task.index}.csv"), emit: id_edges

    script:
    """
    #!/usr/bin/env bash
    set -Eeuo pipefail
    grebi_make_neo_csv \
      --in-graph-metadata-jsons ${graph_metadata_json} \
      --in-nodes-jsonl ${nodes_jsonl} \
      --in-edges-jsonl ${edges_jsonl} \
      --out-nodes-csv-path neo_nodes_${subgraph}_${task.index}.csv \
      --out-edges-csv-path neo_edges_${subgraph}_${task.index}.csv \
      --out-id-edges-csv-path neo_edges_ids_${subgraph}_${task.index}.csv \
      --add-prefix ${subgraph}:
    """
}
