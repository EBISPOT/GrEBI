process merge_graph_metadata_jsons {
    cache "lenient"
    memory "4 GB"
    time "1h"

    input:
    path(graph_metadata_jsons)
    val(subgraph)

    output:
    path("${subgraph}_metadata_merged.json")

    script:
    """
    #!/usr/bin/env bash
    set -Eeuo pipefail
    python3 /opt/grebi_dataload/05_link/merge_graph_metadata_jsons.py ${graph_metadata_jsons} > ${subgraph}_metadata_merged.json
    """
}
