process prepare_postgres_blobs {
    cache "lenient"
    memory "4 GB"
    time "1h"

    input:
    tuple val(subgraph), path(compressed_blob)

    output:
    tuple val(subgraph), path("postgres_blobs_${subgraph}_${task.index}.pgbin"), emit: blobs_pgbin

    script:
    """
    #!/usr/bin/env bash
    set -Eeuo pipefail
    cat ${compressed_blob} | grebi_make_postgres_blobs > postgres_blobs_${subgraph}_${task.index}.pgbin
    """
}
