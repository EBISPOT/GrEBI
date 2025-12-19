process create_compressed_blobs {
    cache "lenient"
    memory "16 GB"
    time "1h"

    input:
    path(mat_jsonl)
    val(subgraph)

    output:
    path("${subgraph}_${task.index}_compressed.blob"), emit: compressed_blob

    script:
    """
    #!/usr/bin/env bash
    set -Eeuo pipefail
    cat ${mat_jsonl} | grebi_make_compressed_blob > ${subgraph}_${task.index}_compressed.blob
    """
}
