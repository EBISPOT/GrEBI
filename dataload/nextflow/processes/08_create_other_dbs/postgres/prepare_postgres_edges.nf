process prepare_postgres_edges {
    cache "lenient"
    memory "4 GB" 
    time "1h"

    input:
    path(edges_jsonl)
    path(graph_metadata_json)
    val(subgraph)

    output:
    path("postgres_edges_${subgraph}_${task.index}.tsv.gz"), emit: edges_tsv
    path("postgres_edges_columns_${subgraph}_${task.index}.txt"), emit: columns

    script:
    """
    #!/usr/bin/env bash
    set -Eeuo pipefail
    grebi_make_postgres_edges \
      --in-edges-jsonl ${edges_jsonl} \
      --in-graph-metadata-json ${graph_metadata_json} \
      --out-edges-tsv-path postgres_edges_${subgraph}_${task.index}.tsv \
      --out-columns-path postgres_edges_columns_${subgraph}_${task.index}.txt
    pigz --best postgres_edges_${subgraph}_${task.index}.tsv
    """
}
