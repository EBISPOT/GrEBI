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
    # xz threads = allocated cpus, NOT -T0: -T0 spawns one thread per MACHINE
    # core, and each -6 thread wants ~100-700 MB — on a shared HPC node that
    # blew straight through the cgroup memory limit (exit 141, SIGPIPE from tar
    # after the OOM-killed xz closed the pipe). Level tunable (-3 faster, -9e
    # max ratio). See package_neo.nf.
    tar --warning=no-file-changed -chf - ${postgres_data} | xz -T${task.cpus} -6 > postgres.tar.xz
    # capture in ONE command: PIPESTATUS is reset by every command, so reading
    # [0] and [1] in separate assignments leaves [1] unbound (set -u aborts)
    rc=("\${PIPESTATUS[@]}")
    tar_rc=\${rc[0]}
    xz_rc=\${rc[1]}
    set -e
    # tar can exit 1 for the benign file-changed warning; xz must succeed outright.
    if [ "\$tar_rc" -gt 1 ] || [ "\$xz_rc" -ne 0 ]; then
        echo "packaging ${postgres_data} failed (tar exit \$tar_rc, xz exit \$xz_rc)" >&2
        exit 1
    fi
    echo "Packaged PostgreSQL data: postgres.tar.xz"
    """
}
