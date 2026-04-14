process create_compressed_blobs {
    cache "lenient"
    memory "16 GB"
    time "1h"

    input:
    tuple val(subgraph), val(blob_kind), val(shard_id), path(mat_jsonl)

    output:
    tuple val(subgraph), val(blob_kind), val(shard_id), path("${subgraph}_${blob_kind}_${shard_id}_compressed.blob"), emit: compressed_blob

    script:
    """
    #!/usr/bin/env bash
    set -Eeuo pipefail
    cat ${mat_jsonl} | grebi_make_compressed_blob > ${subgraph}_${blob_kind}_${shard_id}_compressed.blob
    """
}
