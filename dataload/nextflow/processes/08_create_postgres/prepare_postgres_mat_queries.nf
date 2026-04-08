process prepare_postgres_mat_queries {
    cache "lenient"
    memory "4 GB"
    time "2h"

    input:
    tuple val(subgraph), path(linked_results_jsonl)

    output:
    tuple val(subgraph), path("mat_queries_${subgraph}_${task.index}.tsv.gz"), emit: mat_queries_tsv

    script:
    """
    #!/usr/bin/env bash
    set -Eeuo pipefail

    # Extract query_id from filename: {queryid}.linked_results.jsonl
    QUERY_ID=\$(basename ${linked_results_jsonl} .linked_results.jsonl)

    # Convert JSONL to tab-separated (query_id \\t row_number \\t json_line) for COPY
    awk -v qid="\$QUERY_ID" '{printf "%s\\t%d\\t%s\\n", qid, NR, \$0}' ${linked_results_jsonl} \
        | pigz --best > mat_queries_${subgraph}_${task.index}.tsv.gz
    """
}
