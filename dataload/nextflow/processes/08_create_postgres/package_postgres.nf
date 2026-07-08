process package_postgres {
    cache "lenient"
    memory "4 GB"
    time "24h"
    cpus "16"

    input:
    path(postgres_data)
    val(out_dir)

    publishDir "${out_dir}", overwrite: true

    output:
    path("postgres.tar.xz")

    script:
    """
    #!/usr/bin/env bash
    set -Eeuo pipefail
    # tar can exit 1 ("file changed as we read it") for benign metadata changes
    # on bind-mounted filesystems (e.g. Docker Desktop on macOS) even when the
    # Postgres data dir is quiesced. Tolerate that specific warning (exit 1) but
    # still fail on fatal tar errors (exit >= 2).
    set +e
    # xz -T0 (all cores); level tunable (-3 faster, -9e max ratio). See package_neo.nf.
    tar --warning=no-file-changed -chf - ${postgres_data} | xz -T0 -6 > postgres.tar.xz
    rc=\${PIPESTATUS[0]}
    set -e
    if [ "\$rc" -gt 1 ]; then
        echo "tar failed packaging ${postgres_data} (exit \$rc)" >&2
        exit "\$rc"
    fi
    echo "Packaged PostgreSQL data: postgres.tar.xz"
    """
}
