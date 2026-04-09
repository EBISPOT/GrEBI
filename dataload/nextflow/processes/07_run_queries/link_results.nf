process link_results {
    cache "lenient"
    memory "8 GB" 
    time "8h"
    cpus "4"

    input:
    tuple val(subgraph), path(results_jsonl), path(entity_metadata_jsonl), path(groups_txt)

    output:
    tuple val(subgraph), path("${results_jsonl.simpleName}.linked_results.jsonl")

    script:
    """
    #!/usr/bin/env bash
    set -Eeuo pipefail
    cat ${results_jsonl} | \
    grebi_link_results \
          --in-metadata-jsonl ${entity_metadata_jsonl} \
          --groups-txt ${groups_txt} \
          > ${results_jsonl.simpleName}.linked_results.jsonl
    """
}
