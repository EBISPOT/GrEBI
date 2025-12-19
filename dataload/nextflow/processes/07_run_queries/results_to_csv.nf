process results_to_csv {
    cache "lenient"
    memory "8 GB" 
    time "8h"
    cpus "8"

    input:
    path(results_jsonl)
    val(out_dir)

    publishDir "${out_dir}", overwrite: true

    output:
    path("query_results/${results_jsonl.simpleName}.results.csv.gz")

    script:
    """
    #!/usr/bin/env bash
    set -Eeuo pipefail
    mkdir query_results
    cat ${results_jsonl} | \
    python3 /opt/grebi_dataload/07_run_queries/jsonl_to_csv.py \
    | pigz --best > query_results/${results_jsonl.simpleName}.results.csv.gz
    """
}
