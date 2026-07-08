process package_release {
    cache "lenient"
    memory "4 GB"
    time "24h"
    cpus "16"

    input:
    path(release_dir)
    val(out_dir)

    publishDir "${out_dir}", overwrite: true

    output:
    path("release.tar.xz")

    script:
    """
    #!/usr/bin/env bash
    set -Eeuo pipefail
    # tar can exit 1 ("file changed as we read it") for benign metadata changes
    # on bind-mounted filesystems (e.g. Docker Desktop on macOS). Tolerate that
    # warning (exit 1) but still fail on fatal tar errors (exit >= 2).
    set +e
    # xz -T0 (all cores); level tunable (-3 faster, -9e max ratio). See package_neo.nf.
    tar --warning=no-file-changed -chf release.tar.xz --use-compress-program="xz -T0 -6" ${release_dir}
    rc=\$?
    set -e
    if [ "\$rc" -gt 1 ]; then
        echo "tar failed packaging ${release_dir} (exit \$rc)" >&2
        exit "\$rc"
    fi
    """
}
