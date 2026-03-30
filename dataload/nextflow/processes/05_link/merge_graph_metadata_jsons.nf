process merge_graph_metadata_jsons {
    cache "lenient"
    memory "4 GB"
    time "1h"

    input:
    tuple val(subgraph), path(graph_metadata_jsons), val(downloads_path)

    output:
    tuple val(subgraph), path("${subgraph}_metadata_merged.json")

    script:
    """
    #!/usr/bin/env bash
    set -Eeuo pipefail
    DOWNLOADS_FLAG=""
    if [ -d "${downloads_path}" ]; then
        DOWNLOADS_FLAG="--downloads-dir ${downloads_path}"
    fi
    python3 /opt/grebi_dataload/05_link/merge_graph_metadata_jsons.py ${graph_metadata_jsons} \$DOWNLOADS_FLAG > ${subgraph}_metadata_merged.json
    """
}
