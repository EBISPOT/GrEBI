process prepare_postgres {
    cache "lenient"
    memory "4 GB" 
    time "1h"

    input:
    path(edges_jsonl)
    path(graph_metadata_json)
    val(subgraph)

    output:
    path("postgres_edges_${subgraph}_${task.index}.tsv"), emit: edges_tsv
    path("postgres_schema_${subgraph}_${task.index}.sql"), emit: schema_sql

    script:
    """
    #!/usr/bin/env bash
    set -Eeuo pipefail
    grebi_make_postgres_edges \
      --in-edges-jsonl ${edges_jsonl} \
      --in-graph-metadata-json ${graph_metadata_json} \
      --out-edges-tsv-path postgres_edges_${subgraph}_${task.index}.tsv \
      --out-schema-sql-path postgres_schema_${subgraph}_${task.index}.sql \
      --table-name edges_${subgraph}
    """
}
