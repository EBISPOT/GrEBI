process prepare_postgres_mat_queries {
    cache "lenient"
    memory "4 GB"
    time "2h"

    input:
    tuple val(subgraph), path(linked_results_jsonl)

    output:
    tuple val(subgraph), path("mat_queries_${subgraph}_${task.index}.pgbin"), emit: mat_queries_pgbin

    script:
    """
    #!/usr/bin/env bash
    set -Eeuo pipefail

    # Extract query_id from filename: {queryid}.linked_results.jsonl
    QUERY_ID=\$(basename ${linked_results_jsonl} .linked_results.jsonl)

    grebi_make_postgres_mat_queries --query-id "\$QUERY_ID" < ${linked_results_jsonl} \
        > mat_queries_${subgraph}_${task.index}.pgbin
    """
}
