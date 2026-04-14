process prepare_postgres_edges {
    cache "lenient"
    memory "4 GB" 
    time "1h"

    input:
    tuple val(subgraph), val(shard_id), path(edges_jsonl), path(graph_metadata_json)

    output:
    tuple val(subgraph), val(shard_id), path("postgres_edges_${subgraph}_${shard_id}.pgbin"), emit: edges_pgbin
    tuple val(subgraph), val(shard_id), path("postgres_edges_columns_${subgraph}_${shard_id}.txt"), emit: columns

    script:
    """
    #!/usr/bin/env bash
    set -Eeuo pipefail
    grebi_make_postgres_edges \
      --in-edges-jsonl ${edges_jsonl} \
      --in-graph-metadata-json ${graph_metadata_json} \
      --out-edges-pgbin-path postgres_edges_${subgraph}_${shard_id}.pgbin \
      --out-columns-path postgres_edges_columns_${subgraph}_${shard_id}.txt
    """
}
