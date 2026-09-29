// The table files of a release: each materialised query's results as gzipped
// CSV and as Parquet, under the graph they were materialised in (two graphs
// materialise the same templates, so the query id alone does not name a table).
process make_download_tables {
    cache "lenient"
    memory "8 GB" 
    time "8h"
    cpus "4"

    input:
    tuple val(subgraph), path(results_jsonl), path(metadata_json)
    val(out_dir)

    publishDir "${out_dir}", overwrite: true

    output:
    tuple val(subgraph), path("query_results/${subgraph}/${results_jsonl.simpleName}.csv.gz"), path("query_results/${subgraph}/${results_jsonl.simpleName}.parquet")

    script:
    """
    #!/usr/bin/env bash
    set -Eeuo pipefail
    mkdir -p query_results/${subgraph}
    cat ${results_jsonl} | \
    grebi_make_download_tables \
          --in-metadata-json ${metadata_json} \
          --out-parquet-path query_results/${subgraph}/${results_jsonl.simpleName}.parquet \
    | pigz --best > query_results/${subgraph}/${results_jsonl.simpleName}.csv.gz
    """
}
