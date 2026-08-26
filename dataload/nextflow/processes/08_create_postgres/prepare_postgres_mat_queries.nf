process prepare_postgres_mat_queries {
    cache "lenient"
    memory "4 GB"
    time "2h"

    input:
    tuple val(subgraph), path(linked_results_jsonl), path(query_metadata_json)

    output:
    tuple val(subgraph), path("matq_*.pgbin"), emit: mat_queries_pgbin
    tuple val(subgraph), path("matq_*.columns"), emit: columns
    tuple val(subgraph), path("matq_*.indexes"), emit: indexes

    script:
    """
    #!/usr/bin/env bash
    set -Eeuo pipefail

    # The query metadata names the storage table (matq_{sg}_{query}) and its
    # typed columns; the writer derives the physical schema and emits the
    # pgbin plus .columns/.indexes DDL sidecars, all named after the table.
    grebi_make_postgres_mat_queries --in-metadata-json ${query_metadata_json} \
        < ${linked_results_jsonl}
    """
}
