process create_sqlite {
    cache "lenient"
    memory "4 GB" 
    time "48h"
    cpus "4"
    errorStrategy 'retry'
    maxRetries 10

    input:
    tuple val(subgraph), val(compressed_blobs)
    val(out_dir)

    publishDir "${out_dir}", overwrite: true

    output:
    tuple val(subgraph), path("${subgraph}.sqlite3")

    script:
    """
    #!/usr/bin/env bash
    set -Eeuo pipefail
    cat ${compressed_blobs.iterator().join(" ")} \
        | grebi_make_sqlite \
            --db-path ${subgraph}.sqlite3 \
            --batch-size 450 \
            --page-size 16384 \
            --cache-size 1000000
    """
}
