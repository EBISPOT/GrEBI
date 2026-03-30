process package_release {
    cache "lenient"
    memory "4 GB"
    time "8h"
    cpus "4"

    input:
    path(release_dir)
    val(out_dir)

    publishDir "${out_dir}", overwrite: true

    output:
    path("release.tgz")

    script:
    """
    #!/usr/bin/env bash
    set -Eeuo pipefail
    tar -chf release.tgz --use-compress-program="pigz --fast" ${release_dir}
    """
}
