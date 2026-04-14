process prepare_postgres_nodes {
    cache "lenient"
    memory "4 GB" 
    time "1h"

    input:
    tuple val(subgraph), val(shard_id), path(nodes_jsonl), path(graph_metadata_json)

    output:
    tuple val(subgraph), val(shard_id), path("postgres_nodes_${subgraph}_${shard_id}.pgbin"), emit: nodes_pgbin
    tuple val(subgraph), val(shard_id), path("postgres_nodes_columns_${subgraph}_${shard_id}.txt"), emit: columns

    script:
    """
    #!/usr/bin/env bash
    set -Eeuo pipefail
    grebi_make_postgres_nodes \
      --in-nodes-jsonl ${nodes_jsonl} \
      --in-graph-metadata-json ${graph_metadata_json} \
      --out-nodes-pgbin-path postgres_nodes_${subgraph}_${shard_id}.pgbin \
      --out-columns-path postgres_nodes_columns_${subgraph}_${shard_id}.txt
    """
}
