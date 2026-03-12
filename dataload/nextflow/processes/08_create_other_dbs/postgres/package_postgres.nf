process package_postgres {
    cache "lenient"
    memory "4 GB"
    time "8h"
    cpus "4"

    input:
    path(postgres_data)
    val(subgraph)
    val(out_dir)

    publishDir "${out_dir}", overwrite: true

    output:
    path("${subgraph}_postgres.tgz")

    script:
    """
    #!/usr/bin/env bash
    set -Eeuo pipefail
    tar -chf - ${postgres_data} | pigz > ${subgraph}_postgres.tgz
    echo "Packaged PostgreSQL data: ${subgraph}_postgres.tgz"
    """
}
