process create_neo_ids_csv {
    cache "lenient"
    memory "4 GB" 
    time "48h"
    cpus "4"

    input:
    path(ids_txt)
    val(subgraph)

    output:
    path("neo_nodes_ids_${subgraph}.csv")

    script:
    """
    #!/usr/bin/env bash
    set -Eeuo pipefail
    cat ${ids_txt} | grebi_make_neo_ids_csv > neo_nodes_ids_${subgraph}.csv
    """
}
