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
    path(metadata_jsons)

    output:
    path("postgres_data")

    script:
    """
    #!/usr/bin/env bash
    set -Eeuo pipefail

    # Discover subgraph names from edge pgbin filenames
    # Filenames: postgres_edges_{SUBGRAPH}_{index}.pgbin
    SUBGRAPHS=(\$(ls postgres_edges_*.pgbin | sed -E 's/^postgres_edges_(.*)_[0-9]+\\.pgbin\$/\\1/' | sort -u))
    echo "Discovered subgraphs: \${SUBGRAPHS[*]}"

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

    PSQL="psql -h \$PGSOCK -p \$PGPORT -U \$PGUSER -d grebi"

    \$PSQL -c "CREATE EXTENSION IF NOT EXISTS vector;"
    \$PSQL -c "CREATE EXTENSION IF NOT EXISTS pg_trgm;"

    # Helper for COPY from binary pgbin files
    _pg_import_binary() {
        local TABLE=\$1
        local BIN_FILE=\$2
        local ABSFILE
        ABSFILE=\$(readlink -f "\$BIN_FILE")
        echo "Importing binary \$BIN_FILE into \$TABLE ..."
        psql -h \$PGSOCK -p \$PGPORT -U \$PGUSER -d grebi -c "COPY \\"\$TABLE\\" FROM '\$ABSFILE' WITH (FORMAT binary)"
    }
    export -f _pg_import_binary

    # Helper for parallel COPY from binary pgbin files
    _pg_import_binary() {
        local TABLE=\$1
        local BIN_FILE=\$2
        local ABSFILE
        ABSFILE=\$(readlink -f "\$BIN_FILE")
        echo "Importing binary \$BIN_FILE into \$TABLE ..."
        psql -h \$PGSOCK -p \$PGPORT -U \$PGUSER -d grebi -c "COPY \\"\$TABLE\\" FROM '\$ABSFILE' WITH (FORMAT binary)"
    }
    export -f _pg_import_binary

    # === Process each subgraph ===
    for SG in "\${SUBGRAPHS[@]}"; do
        echo "=== Processing subgraph: \$SG ==="

        # --- EDGES TABLE ---
        EDGES_COLS_FILES=(postgres_edges_columns_\${SG}_*.txt)
        EDGES_COLS_FILE="\${EDGES_COLS_FILES[0]}"
        EDGES_COLS=\$(paste -sd',' < "\$EDGES_COLS_FILE")

        \$PSQL -c "
            CREATE TABLE \\"edges_\${SG}\\" (
                \$EDGES_COLS
            ) WITH (fillfactor=100);
        "

        EDGE_WORKERS=\$((NPROC < 8 ? NPROC : 8))
        echo "Importing \$(ls postgres_edges_\${SG}_*.pgbin | wc -l) edge files for \$SG with \$EDGE_WORKERS parallel workers..."
        printf '%s\\0' postgres_edges_\${SG}_*.pgbin | \\
            xargs -0 -P \$EDGE_WORKERS -n1 bash -c "set -e; _pg_import_binary \\"edges_\${SG}\\" \\"\\\$1\\"" _

        echo "Creating edge indexes for \$SG (parallel)..."
        \$PSQL -c "CREATE INDEX \\"idx_edges_\${SG}_edgeId\\" ON \\"edges_\${SG}\\" USING btree (\\"grebi:edgeId\\");" &
        \$PSQL -c "CREATE INDEX \\"idx_edges_\${SG}_fromNodeId\\" ON \\"edges_\${SG}\\" USING btree (\\"grebi:fromNodeId\\");" &
        \$PSQL -c "CREATE INDEX \\"idx_edges_\${SG}_toNodeId\\" ON \\"edges_\${SG}\\" USING btree (\\"grebi:toNodeId\\");" &
        \$PSQL -c "CREATE INDEX \\"idx_edges_\${SG}_type\\" ON \\"edges_\${SG}\\" USING btree (\\"grebi:type\\");" &
        \$PSQL -c "CREATE INDEX \\"idx_edges_\${SG}_datasources_gin\\" ON \\"edges_\${SG}\\" USING gin (\\"grebi:datasources\\");" &
        wait

        \$PSQL -c "ANALYZE \\"edges_\${SG}\\";"

        # --- NODES TABLE ---
        NODES_COLS_FILES=(postgres_nodes_columns_\${SG}_*.txt)
        NODES_COLS_FILE="\${NODES_COLS_FILES[0]}"
        NODES_COLS=\$(paste -sd',' < "\$NODES_COLS_FILE")

        \$PSQL -c "
            CREATE TABLE \\"nodes_\${SG}\\" (
                \$NODES_COLS
            ) WITH (fillfactor=100);
        "

        echo "Importing \$(ls postgres_nodes_\${SG}_*.pgbin | wc -l) node files for \$SG with \$NPROC parallel workers..."
        printf '%s\\0' postgres_nodes_\${SG}_*.pgbin | \\
            xargs -0 -P \$NPROC -n1 bash -c "set -e; _pg_import_binary \\"nodes_\${SG}\\" \\"\\\$1\\"" _

        echo "Creating node indexes for \$SG in parallel..."
        \$PSQL -c "CREATE INDEX \\"idx_nodes_\${SG}_name\\" ON \\"nodes_\${SG}\\" USING btree (\\"grebi:name\\");" &
        \$PSQL -c "CREATE INDEX \\"idx_nodes_\${SG}_name_trgm\\" ON \\"nodes_\${SG}\\" USING gin (\\"grebi:name\\" gin_trgm_ops);" &
        \$PSQL -c "CREATE INDEX \\"idx_nodes_\${SG}_type_gin\\" ON \\"nodes_\${SG}\\" USING gin (\\"grebi:type\\");" &
        \$PSQL -c "CREATE INDEX \\"idx_nodes_\${SG}_datasources_gin\\" ON \\"nodes_\${SG}\\" USING gin (\\"grebi:datasources\\");" &
        for COL_NAME in \$(grep '^"embedding:' "\$NODES_COLS_FILE" | sed 's/^"\\([^"]*\\)".*/\\1/'); do
            SAFE_MODEL=\$(echo "\$COL_NAME" | sed 's/embedding://' | tr -- '-.' '__')
            \$PSQL -c "CREATE INDEX \\"idx_nodes_\${SG}_embedding_\${SAFE_MODEL}\\" ON \\"nodes_\${SG}\\" USING hnsw (\\"\${COL_NAME}\\" vector_cosine_ops);" &
        done
        wait

        \$PSQL -c "ANALYZE \\"nodes_\${SG}\\";"

        # --- BLOBS TABLE ---
        \$PSQL -c "
            CREATE TABLE \\"blobs_\${SG}\\" (
                id bytea NOT NULL,
                json bytea NOT NULL
            );
        "

        BLOBS_FILES=(postgres_blobs_\${SG}_*.pgbin)
        if [ -f "\${BLOBS_FILES[0]}" ]; then
            BLOB_WORKERS=\$((NPROC < 8 ? NPROC : 8))
            echo "Importing \${#BLOBS_FILES[@]} blob files for \$SG with \$BLOB_WORKERS parallel workers..."
            printf '%s\\0' postgres_blobs_\${SG}_*.pgbin | \\
                xargs -0 -P \$BLOB_WORKERS -n1 bash -c "set -e; _pg_import_binary \\"blobs_\${SG}\\" \\"\\\$1\\"" _
        fi

        echo "Creating blobs primary key for \$SG ..."
        \$PSQL -c "ALTER TABLE \\"blobs_\${SG}\\" ADD PRIMARY KEY (id);"
        \$PSQL -c "ANALYZE \\"blobs_\${SG}\\";"

        # --- AUTOCOMPLETE TABLE ---
        \$PSQL -c "
            CREATE TABLE \\"autocomplete_\${SG}\\" (
                label TEXT NOT NULL
            ) WITH (fillfactor=100);
        "

        AUTOCOMPLETE_FILES=(autocomplete_\${SG}_*.pgbin)
        if [ -f "\${AUTOCOMPLETE_FILES[0]}" ]; then
            echo "Importing \${#AUTOCOMPLETE_FILES[@]} autocomplete files for \$SG..."
            printf '%s\\0' autocomplete_\${SG}_*.pgbin | \\
                xargs -0 -P \$NPROC -n1 bash -c "set -e; _pg_import_binary \\"autocomplete_\${SG}\\" \\"\\\$1\\"" _
        fi

        echo "Creating autocomplete indexes for \$SG..."
        \$PSQL -c "CREATE INDEX \\"idx_autocomplete_\${SG}_trgm\\" ON \\"autocomplete_\${SG}\\" USING gin (label gin_trgm_ops);"
        \$PSQL -c "ANALYZE \\"autocomplete_\${SG}\\";"

        # --- MATERIALISED QUERIES TABLE ---
        \$PSQL -c "
            CREATE TABLE \\"materialised_queries_\${SG}\\" (
                query_id TEXT NOT NULL,
                row_number INT NOT NULL,
                data JSONB NOT NULL
            ) WITH (fillfactor=100);
        "

        MAT_QUERIES_FILES=(mat_queries_\${SG}_*.pgbin)
        if [ -f "\${MAT_QUERIES_FILES[0]}" ]; then
            echo "Importing \${#MAT_QUERIES_FILES[@]} materialised query files for \$SG..."
            printf '%s\\0' mat_queries_\${SG}_*.pgbin | \\
                xargs -0 -P \$NPROC -n1 bash -c "set -e; _pg_import_binary \\"materialised_queries_\${SG}\\" \\"\\\$1\\"" _
        fi

        echo "Creating materialised query indexes for \$SG..."
        \$PSQL -c "CREATE INDEX \\"idx_mat_queries_\${SG}_query_id\\" ON \\"materialised_queries_\${SG}\\" USING btree (query_id);"
        \$PSQL -c "CREATE INDEX \\"idx_mat_queries_\${SG}_query_id_row\\" ON \\"materialised_queries_\${SG}\\" USING btree (query_id, row_number);"
        \$PSQL -c "ANALYZE \\"materialised_queries_\${SG}\\";"
    done

    # --- GRAPH METADATA TABLE (one row per graph, JSONB) ---
    \$PSQL -c "
        CREATE TABLE graph_metadata (
            graph TEXT PRIMARY KEY,
            metadata JSONB NOT NULL
        );
    "

    echo "Writing graph_metadata.tsv ..."
    python3 -c "
import json, csv, sys, glob
with open('graph_metadata.tsv', 'w', newline='') as f:
    w = csv.writer(f, delimiter='\\t')
    for mf in sorted(glob.glob('*_metadata.json')):
        sg = mf.removesuffix('_metadata.json')
        with open(mf) as fi:
            w.writerow([sg, json.dumps(json.load(fi), separators=(',',':'))])
"
    echo "Loading graph_metadata.tsv ..."
    \$PSQL -c "COPY graph_metadata FROM '\$PWD/graph_metadata.tsv' WITH (FORMAT csv, DELIMITER E'\\t');"
    \$PSQL -c "ANALYZE graph_metadata;"

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

    echo "PostgreSQL data directory built: \$PGDATA (subgraphs: \${SUBGRAPHS[*]})"
    """
}
