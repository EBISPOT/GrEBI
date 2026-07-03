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
    # tar can exit 1 ("file changed as we read it") for benign metadata changes
    # on bind-mounted filesystems (e.g. Docker Desktop on macOS). Tolerate that
    # warning (exit 1) but still fail on fatal tar errors (exit >= 2).
    set +e
    tar --warning=no-file-changed -chf release.tgz --use-compress-program="pigz --fast" ${release_dir}
    rc=\$?
    set -e
    if [ "\$rc" -gt 1 ]; then
        echo "tar failed packaging ${release_dir} (exit \$rc)" >&2
        exit "\$rc"
    fi
    """
}
