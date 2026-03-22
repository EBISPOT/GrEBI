process prepare_postgres_nodes {
    cache "lenient"
    memory "4 GB" 
    time "1h"

    input:
    path(nodes_jsonl)
    path(graph_metadata_json)
    val(subgraph)

    output:
    path("postgres_nodes_${subgraph}_${task.index}.tsv"), emit: nodes_tsv
    path("postgres_nodes_schema_${subgraph}_${task.index}.sql"), emit: schema_sql

    script:
    """
    #!/usr/bin/env bash
    set -Eeuo pipefail
    grebi_make_postgres_nodes \
      --in-nodes-jsonl ${nodes_jsonl} \
      --in-graph-metadata-json ${graph_metadata_json} \
      --out-nodes-tsv-path postgres_nodes_${subgraph}_${task.index}.tsv \
      --out-schema-sql-path postgres_nodes_schema_${subgraph}_${task.index}.sql \
      --table-name nodes_${subgraph}
    """
}
