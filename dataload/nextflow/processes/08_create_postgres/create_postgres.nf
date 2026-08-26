process create_postgres {
    cache "lenient"
    memory "32 GB"
    time "48h"
    cpus "8"

    input:
    path(edges_pgbins)
    path(edges_columns)
    path(nodes_pgbins)
    path(nodes_columns)
    path(blobs_pgbins)
    path(autocomplete_pgbins)
    path(mat_queries_pgbins)
    path(mat_queries_columns)
    path(mat_queries_indexes)
    path(metadata_jsons)

    output:
    path("postgres_data")

    script:
    """
    #!/usr/bin/env bash
    set -Eeuo pipefail

    # Ensure current UID has an entry in /etc/passwd (required by initdb)
    if ! getent passwd \$(id -u) > /dev/null 2>&1; then
        echo "grebi:x:\$(id -u):\$(id -g):PostgreSQL:/tmp:/bin/bash" >> /etc/passwd
    fi

    export PGDATA=\$PWD/postgres_data
    export PGPORT=5433
    export PGUSER=grebi
    export PGSOCK=/tmp/pg_sock_\$\$
    mkdir -p \$PGSOCK

    NPROC=\$(nproc)
    echo "Using \$NPROC parallel workers, PGDATA=\$PGDATA"

    trap 'pg_ctl -D \$PGDATA stop -m immediate 2>/dev/null || true' EXIT

    # Initialise a fresh PostgreSQL data directory
    initdb --username=\$PGUSER --auth=trust --no-locale --encoding=UTF8 -D \$PGDATA

    # Tune for bulk import
    cat >> \$PGDATA/postgresql.conf <<EOF
listen_addresses = ''
unix_socket_directories = '\$PGSOCK'
shared_buffers = ${params.pg_build_shared_buffers}
work_mem = ${params.pg_build_work_mem}
maintenance_work_mem = ${params.pg_build_maintenance_work_mem}
max_wal_size = ${params.pg_build_max_wal_size}
wal_level = minimal
max_wal_senders = 0
fsync = off
synchronous_commit = off
full_page_writes = off
checkpoint_completion_target = 0.9
autovacuum = off
max_connections = 200
max_worker_processes = \$((NPROC + 4))
max_parallel_maintenance_workers = \$((NPROC < 12 ? NPROC : 12))
effective_io_concurrency = 200
huge_pages = try
effective_cache_size = ${params.pg_build_effective_cache_size}
default_toast_compression = 'lz4'
EOF

    # Start PostgreSQL locally (unix socket only)
    pg_ctl -D \$PGDATA -l \$PWD/pg_startup.log start -o "-p \$PGPORT -k \$PGSOCK" || {
        echo "=== PostgreSQL startup log ===" >&2
        cat \$PWD/pg_startup.log >&2
        exit 1
    }

    # Wait for postgres to start
    for i in \$(seq 1 30); do
        if pg_isready -h \$PGSOCK -p \$PGPORT -U \$PGUSER; then
            break
        fi
        sleep 1
    done

    createdb -h \$PGSOCK -p \$PGPORT -U \$PGUSER grebi

    # Load all data using Python script with COPY FREEZE
    export PGHOST=\$PGSOCK
    export PGDATABASE=grebi
    python3 ${projectDir}/processes/08_create_postgres/load_postgres.py --local

    # Stop PostgreSQL cleanly
    pg_ctl -D \$PGDATA stop -m fast

    # Re-enable WAL and update settings for production use
    _TMPCONF=\$(mktemp)
    sed \\
      -e 's/^wal_level = minimal/wal_level = replica/' \\
      -e 's/^fsync = off/fsync = on/' \\
      -e 's/^synchronous_commit = off/synchronous_commit = on/' \\
      -e 's/^full_page_writes = off/full_page_writes = on/' \\
      -e 's/^autovacuum = off/autovacuum = on/' \\
      -e "s|unix_socket_directories = .*|unix_socket_directories = '/var/run/postgresql'|" \\
      -e "s/^listen_addresses = ''/listen_addresses = '*'/" \\
      -e "s/^shared_buffers = .*/shared_buffers = ${params.pg_shared_buffers}/" \\
      -e "s/^work_mem = .*/work_mem = ${params.pg_work_mem}/" \\
      -e "s/^maintenance_work_mem = .*/maintenance_work_mem = ${params.pg_maintenance_work_mem}/" \\
      -e "s/^max_wal_size = .*/max_wal_size = ${params.pg_max_wal_size}/" \\
      -e '/^huge_pages = /d' \\
      -e '/^effective_cache_size = /d' \\
      -e 's/^max_parallel_maintenance_workers = .*/max_parallel_maintenance_workers = 2/' \\
      \$PGDATA/postgresql.conf > \$_TMPCONF
    cat \$_TMPCONF > \$PGDATA/postgresql.conf
    rm -f \$_TMPCONF

    # Add pg_hba.conf entry for network connections
    echo "host all all 0.0.0.0/0 trust" >> \$PGDATA/pg_hba.conf

    chmod -R a+rX \$PGDATA

    echo "PostgreSQL data directory built: \$PGDATA"
    """
}
