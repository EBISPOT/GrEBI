process create_postgres {
    cache "lenient"
    memory "32 GB"
    time "48h"
    cpus "8"

    input:
    path(edges_tsvs)
    path(edges_columns)
    path(nodes_tsvs)
    path(nodes_columns)
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

    PSQL="psql -h \$PGSOCK -p \$PGPORT -U \$PGUSER -d grebi"

    # === EDGES TABLE ===
    EDGES_COLS_FILES=(postgres_edges_columns_${subgraph}_*.txt)
    EDGES_COLS_FILE="\${EDGES_COLS_FILES[0]}"
    EDGES_COLS=\$(paste -sd',' < "\$EDGES_COLS_FILE")

    \$PSQL -c "
        CREATE UNLOGGED TABLE \\"edges_${subgraph}\\" (
            \$EDGES_COLS
        ) WITH (fillfactor=100);
    "

    # Helper for parallel COPY from gzipped files
    _pg_import() {
        local TABLE=\$1
        local GZ_FILE=\$2
        local ABSFILE
        ABSFILE=\$(readlink -f "\$GZ_FILE")
        echo "Importing \$TABLE \$GZ_FILE ..."
        psql -h \$PGSOCK -p \$PGPORT -U \$PGUSER -d grebi -c "COPY \\"\$TABLE\\" FROM PROGRAM 'pigz -dc \$ABSFILE' WITH (FORMAT text)"
    }
    export -f _pg_import

    # Cap edge COPY parallelism at 8 to reduce contention
    EDGE_WORKERS=\$((NPROC < 8 ? NPROC : 8))
    echo "Importing \$(ls postgres_edges_${subgraph}_*.tsv.gz | wc -l) edge files with \$EDGE_WORKERS parallel workers..."
    printf '%s\\0' postgres_edges_${subgraph}_*.tsv.gz | \\
        xargs -0 -P \$EDGE_WORKERS -n1 bash -c 'set -e; _pg_import "edges_${subgraph}" "\$1"' _

    echo "Creating edge hash indexes (parallel)..."
    \$PSQL -c "CREATE INDEX \\"idx_edges_${subgraph}_edgeId\\" ON \\"edges_${subgraph}\\" USING hash (\\"grebi:edgeId\\");" &
    \$PSQL -c "CREATE INDEX \\"idx_edges_${subgraph}_fromNodeId\\" ON \\"edges_${subgraph}\\" USING hash (\\"grebi:fromNodeId\\");" &
    \$PSQL -c "CREATE INDEX \\"idx_edges_${subgraph}_toNodeId\\" ON \\"edges_${subgraph}\\" USING hash (\\"grebi:toNodeId\\");" &
    \$PSQL -c "CREATE INDEX \\"idx_edges_${subgraph}_type_hash\\" ON \\"edges_${subgraph}\\" USING hash (\\"grebi:type\\");" &
    \$PSQL -c "CREATE INDEX \\"idx_edges_${subgraph}_type_btree\\" ON \\"edges_${subgraph}\\" USING btree (\\"grebi:type\\");" &
    \$PSQL -c "CREATE INDEX \\"idx_edges_${subgraph}_datasources_gin\\" ON \\"edges_${subgraph}\\" USING gin (\\"grebi:datasources\\");" &
    wait

    \$PSQL -c "ANALYZE \\"edges_${subgraph}\\";"

    # === NODES TABLE (with pgvector) ===
    NODES_COLS_FILES=(postgres_nodes_columns_${subgraph}_*.txt)
    NODES_COLS_FILE="\${NODES_COLS_FILES[0]}"
    NODES_COLS=\$(paste -sd',' < "\$NODES_COLS_FILE")

    \$PSQL -c "CREATE EXTENSION IF NOT EXISTS vector;"

    \$PSQL -c "
        CREATE UNLOGGED TABLE \\"nodes_${subgraph}\\" (
            \$NODES_COLS
        ) WITH (fillfactor=100);
    "

    echo "Importing \$(ls postgres_nodes_${subgraph}_*.tsv.gz | wc -l) node files with \$NPROC parallel workers..."
    printf '%s\\0' postgres_nodes_${subgraph}_*.tsv.gz | \\
        xargs -0 -P \$NPROC -n1 bash -c 'set -e; _pg_import "nodes_${subgraph}" "\$1"' _

    echo "Creating node indexes in parallel..."
    # Hash index for name lookups
    \$PSQL -c "CREATE INDEX \\"idx_nodes_${subgraph}_name\\" ON \\"nodes_${subgraph}\\" USING hash (\\"grebi:name\\");" &
    # HNSW indexes for embedding columns (identified by column name pattern)
    for COL_NAME in \$(grep '^"embedding:' "\$NODES_COLS_FILE" | sed 's/^"\\([^"]*\\)".*/\\1/'); do
        SAFE_MODEL=\$(echo "\$COL_NAME" | sed 's/embedding://' | tr -- '-.' '__')
        \$PSQL -c "CREATE INDEX \\"idx_nodes_${subgraph}_embedding_\${SAFE_MODEL}\\" ON \\"nodes_${subgraph}\\" USING hnsw (\\"\${COL_NAME}\\" vector_cosine_ops);" &
    done
    wait

    \$PSQL -c "ANALYZE \\"nodes_${subgraph}\\";"

    # Convert UNLOGGED tables back to LOGGED for production durability
    echo "Converting tables to LOGGED..."
    \$PSQL -c "ALTER TABLE \\"edges_${subgraph}\\" SET LOGGED;"
    \$PSQL -c "ALTER TABLE \\"nodes_${subgraph}\\" SET LOGGED;"

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
