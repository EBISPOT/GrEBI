process prepare_postgres_nodes {
    cache "lenient"
    memory "4 GB" 
    time "1h"

    input:
    tuple val(subgraph), path(nodes_jsonl), path(graph_metadata_json)

    output:
    tuple val(subgraph), path("postgres_nodes_${subgraph}_${task.index}.tsv.gz"), emit: nodes_tsv
    tuple val(subgraph), path("postgres_nodes_columns_${subgraph}_${task.index}.txt"), emit: columns

    script:
    """
    #!/usr/bin/env bash
    set -Eeuo pipefail
    grebi_make_postgres_nodes \
      --in-nodes-jsonl ${nodes_jsonl} \
      --in-graph-metadata-json ${graph_metadata_json} \
      --out-nodes-tsv-path postgres_nodes_${subgraph}_${task.index}.tsv \
      --out-columns-path postgres_nodes_columns_${subgraph}_${task.index}.txt
    pigz --best postgres_nodes_${subgraph}_${task.index}.tsv
    """
}
