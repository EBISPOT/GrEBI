process create_postgres {
    cache "lenient"
    memory "32 GB"
    time "23h"
    cpus "8"

    input:
    path(edges_tsvs)
    path(edges_schema_sqls)
    path(nodes_tsvs)
    path(nodes_schema_sqls)
    val(subgraph)

    output:
    path("postgres_data_${subgraph}")

    script:
    """
    #!/usr/bin/env bash
    set -Eeuo pipefail

    # Ensure current UID has an entry in /etc/passwd (required by initdb)
    if ! getent passwd \$(id -u) > /dev/null 2>&1; then
        echo "grebi:x:\$(id -u):\$(id -g):PostgreSQL:/tmp:/bin/bash" >> /etc/passwd
    fi

    export PGDATA=\$PWD/postgres_data_${subgraph}
    export PGPORT=5433
    export PGUSER=grebi
    export PGSOCK=/tmp/pg_sock_\$\$
    mkdir -p \$PGSOCK

    NPROC=\$(nproc)
    echo "Using \$NPROC parallel workers"

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

    # === EDGES TABLE ===
    EDGES_SCHEMA_FILES=(postgres_schema_${subgraph}_*.sql)
    EDGES_SCHEMA_FILE=\${EDGES_SCHEMA_FILES[0]}
    psql -h \$PGSOCK -p \$PGPORT -U \$PGUSER -d grebi -f "\$EDGES_SCHEMA_FILE"

    # Drop indexes for faster import
    psql -h \$PGSOCK -p \$PGPORT -U \$PGUSER -d grebi -c "
        DROP INDEX IF EXISTS idx_edges_${subgraph}_fromNodeId;
        DROP INDEX IF EXISTS idx_edges_${subgraph}_toNodeId;
        DROP INDEX IF EXISTS idx_edges_${subgraph}_type;
    "

    # Helper for parallel server-side COPY
    _pg_import() {
        local TABLE=\$1
        local TSV_FILE=\$2
        local ABSFILE
        ABSFILE=\$(readlink -f "\$TSV_FILE")
        echo "Importing \$TABLE \$TSV_FILE ..."
        psql -h \$PGSOCK -p \$PGPORT -U \$PGUSER -d grebi -c "COPY \\"\$TABLE\\" FROM '\$ABSFILE' WITH (FORMAT text)"
    }
    export -f _pg_import

    echo "Importing \$(ls postgres_edges_${subgraph}_*.tsv | wc -l) edge files with \$NPROC parallel workers..."
    printf '%s\\0' postgres_edges_${subgraph}_*.tsv | \\
        xargs -0 -P \$NPROC -n1 bash -c 'set -e; _pg_import "edges_${subgraph}" "\$1"' _

    echo "Creating edge indexes..."
    psql -h \$PGSOCK -p \$PGPORT -U \$PGUSER -d grebi -c "
        CREATE INDEX idx_edges_${subgraph}_fromNodeId ON \\"edges_${subgraph}\\" (\\"grebi:fromNodeId\\");
        CREATE INDEX idx_edges_${subgraph}_toNodeId ON \\"edges_${subgraph}\\" (\\"grebi:toNodeId\\");
        CREATE INDEX idx_edges_${subgraph}_type ON \\"edges_${subgraph}\\" (\\"grebi:type\\");
    "

    psql -h \$PGSOCK -p \$PGPORT -U \$PGUSER -d grebi -c "ANALYZE \\"edges_${subgraph}\\";"

    # === NODES TABLE (with pgvector) ===
    NODES_SCHEMA_FILES=(postgres_nodes_schema_${subgraph}_*.sql)
    NODES_SCHEMA_FILE=\${NODES_SCHEMA_FILES[0]}
    psql -h \$PGSOCK -p \$PGPORT -U \$PGUSER -d grebi -f "\$NODES_SCHEMA_FILE"

    # Drop HNSW indexes for faster bulk import (they will be in the schema SQL)
    psql -h \$PGSOCK -p \$PGPORT -U \$PGUSER -d grebi -t -A -c "
        SELECT indexname FROM pg_indexes WHERE tablename = 'nodes_${subgraph}' AND indexname LIKE 'idx_%';
    " | while read -r idx; do
        [ -z "\$idx" ] && continue
        psql -h \$PGSOCK -p \$PGPORT -U \$PGUSER -d grebi -c "DROP INDEX IF EXISTS \\"\$idx\\";" 2>/dev/null || true
    done

    echo "Importing \$(ls postgres_nodes_${subgraph}_*.tsv | wc -l) node files with \$NPROC parallel workers..."
    printf '%s\\0' postgres_nodes_${subgraph}_*.tsv | \\
        xargs -0 -P \$NPROC -n1 bash -c 'set -e; _pg_import "nodes_${subgraph}" "\$1"' _

    echo "Creating node indexes (including HNSW vector indexes)..."
    grep -i 'CREATE INDEX' "\$NODES_SCHEMA_FILE" | while read -r line; do
        psql -h \$PGSOCK -p \$PGPORT -U \$PGUSER -d grebi -c "\$line" 2>/dev/null || true
    done

    psql -h \$PGSOCK -p \$PGPORT -U \$PGUSER -d grebi -c "ANALYZE \\"nodes_${subgraph}\\";"

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

    # Make data directory readable so downstream packaging steps can tar it
    chmod -R a+rX \$PGDATA

    echo "PostgreSQL data directory built: \$PGDATA"
    """
}
