process results_to_csv {
    cache "lenient"
    memory "8 GB" 
    time "8h"
    cpus "4"

    input:
    tuple val(subgraph), path(results_jsonl)
    val(out_dir)

    publishDir "${out_dir}", overwrite: true

    output:
    tuple val(subgraph), path("query_results/${results_jsonl.simpleName}.results.csv.gz")

    script:
    """
    #!/usr/bin/env bash
    set -Eeuo pipefail
    mkdir query_results
    cat ${results_jsonl} | \
    grebi_jsonl2csv \
    | pigz --best > query_results/${results_jsonl.simpleName}.results.csv.gz
    """
}
