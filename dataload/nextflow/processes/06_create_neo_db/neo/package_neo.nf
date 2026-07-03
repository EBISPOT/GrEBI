process package_neo {
    cache "lenient"
    memory "4 GB" 
    time "8h"
    cpus "4"

    input: 
    tuple val(subgraph), path(neo4j_dir)
    val(out_dir)

    publishDir "${out_dir}", overwrite: true

    output:
    tuple val(subgraph), path("${subgraph}_neo4j.tgz")

    script:
    """
    #!/usr/bin/env bash
    set -Eeuo pipefail
    # tar can exit 1 ("file changed as we read it") for benign metadata changes
    # on bind-mounted filesystems (e.g. Docker Desktop on macOS). Tolerate that
    # warning (exit 1) but still fail on fatal tar errors (exit >= 2).
    set +e
    tar --warning=no-file-changed -chf ${subgraph}_neo4j.tgz --use-compress-program="pigz --fast" ${neo4j_dir}
    rc=\$?
    set -e
    if [ "\$rc" -gt 1 ]; then
        echo "tar failed packaging ${neo4j_dir} (exit \$rc)" >&2
        exit "\$rc"
    fi
    """
}
