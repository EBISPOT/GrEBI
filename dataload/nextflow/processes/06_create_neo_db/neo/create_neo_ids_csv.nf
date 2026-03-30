process create_neo_ids_csv {
    cache "lenient"
    memory "4 GB" 
    time "48h"
    cpus "4"

    input:
    tuple val(subgraph), path(ids_txt)

    output:
    tuple val(subgraph), path("neo_nodes_ids_${subgraph}.csv")

    script:
    """
    #!/usr/bin/env bash
    set -Eeuo pipefail
    cat ${ids_txt} | grebi_make_neo_ids_csv > neo_nodes_ids_${subgraph}.csv
    """
}
