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
    # -S (--sparse): Neo4j store files are largely zero-padded; without this tar
    # records the zeros literally and extraction inflates the store ~5-8x (e.g. a
    # 208G store ballooning past 1T). With -S the archive stores holes sparsely and
    # extraction recreates them, keeping the extracted store small.
    tar --warning=no-file-changed -cShf ${subgraph}_neo4j.tgz --use-compress-program="pigz --fast" ${neo4j_dir}
    rc=\$?
    set -e
    if [ "\$rc" -gt 1 ]; then
        echo "tar failed packaging ${neo4j_dir} (exit \$rc)" >&2
        exit "\$rc"
    fi
    """
}
