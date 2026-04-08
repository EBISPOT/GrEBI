process populate_external_postgres {
    cache "lenient"
    memory "32 GB"
    time "48h"
    cpus "8"

    input:
    path(edges_tsvs)
    path(edges_columns)
    path(nodes_tsvs)
    path(nodes_columns)
    path(blobs_pgbins)
    path(autocomplete_tsvs)
    path(mat_queries_tsvs)

    output:
    path("postgres_external_done")

    script:
    """
    #!/usr/bin/env bash
    set -Eeuo pipefail

    # Validate required env vars
    : "\${PGHOST:?PGHOST must be set}"
    : "\${PGDATABASE:?PGDATABASE must be set}"
    : "\${PGUSER:?PGUSER must be set}"
    export PGPORT="\${PGPORT:-5432}"
    export PGPASSWORD="\${PGPASSWORD:-}"
    export PGSSLMODE="\${PGSSLMODE:-}"

    echo "=== Connecting to \${PGUSER}@\${PGHOST}:\${PGPORT}/\${PGDATABASE} ==="

    if ! psql -c "SELECT 1" >/dev/null 2>&1; then
        echo "ERROR: Cannot connect to PostgreSQL at \${PGHOST}:\${PGPORT}/\${PGDATABASE} as \${PGUSER}"
        echo "Check PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD and network connectivity."
        exit 1
    fi
    echo "Connection OK."

    # Discover subgraph names from edge TSV filenames
    # Filenames: postgres_edges_{SUBGRAPH}_{index}.tsv.gz
    SUBGRAPHS=(\$(ls postgres_edges_*.tsv.gz | sed -E 's/^postgres_edges_(.*)_[0-9]+\\.tsv\\.gz\$/\\1/' | sort -u))
    echo "Discovered subgraphs: \${SUBGRAPHS[*]}"

    NPROC=\$(nproc)
    PSQL="psql -v ON_ERROR_STOP=1"

    \$PSQL -c "CREATE EXTENSION IF NOT EXISTS vector;"
    \$PSQL -c "CREATE EXTENSION IF NOT EXISTS pg_trgm;"

    # Helper for parallel COPY from gzipped files (client-side via \\COPY)
    _pg_import() {
        local TABLE=\$1
        local GZ_FILE=\$2
        echo "Importing \$TABLE \$GZ_FILE ..."
        pigz -dc "\$GZ_FILE" | psql -v ON_ERROR_STOP=1 -c "\\\\COPY \\"\$TABLE\\" FROM STDIN WITH (FORMAT text)"
    }
    export -f _pg_import
    export PGHOST PGPORT PGDATABASE PGUSER PGPASSWORD PGSSLMODE

    # === Process each subgraph ===
    for SG in "\${SUBGRAPHS[@]}"; do
        echo "=== Processing subgraph: \$SG ==="

        # --- EDGES TABLE ---
        EDGES_COLS_FILES=(postgres_edges_columns_\${SG}_*.txt)
        EDGES_COLS_FILE="\${EDGES_COLS_FILES[0]}"
        EDGES_COLS=\$(paste -sd',' < "\$EDGES_COLS_FILE")

        \$PSQL -c "DROP TABLE IF EXISTS \\"edges_\${SG}\\" CASCADE;"
        \$PSQL -c "
            CREATE TABLE \\"edges_\${SG}\\" (
                \$EDGES_COLS
            ) WITH (fillfactor=100);
        "

        EDGE_WORKERS=\$((NPROC < 8 ? NPROC : 8))
        echo "Importing \$(ls postgres_edges_\${SG}_*.tsv.gz | wc -l) edge files for \$SG with \$EDGE_WORKERS parallel workers..."
        printf '%s\\0' postgres_edges_\${SG}_*.tsv.gz | \\
            xargs -0 -P \$EDGE_WORKERS -n1 bash -c "set -e; _pg_import \\"edges_\${SG}\\" \\"\\\$1\\"" _

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

        \$PSQL -c "DROP TABLE IF EXISTS \\"nodes_\${SG}\\" CASCADE;"
        \$PSQL -c "
            CREATE TABLE \\"nodes_\${SG}\\" (
                \$NODES_COLS
            ) WITH (fillfactor=100);
        "

        echo "Importing \$(ls postgres_nodes_\${SG}_*.tsv.gz | wc -l) node files for \$SG with \$NPROC parallel workers..."
        printf '%s\\0' postgres_nodes_\${SG}_*.tsv.gz | \\
            xargs -0 -P \$NPROC -n1 bash -c "set -e; _pg_import \\"nodes_\${SG}\\" \\"\\\$1\\"" _

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
        \$PSQL -c "DROP TABLE IF EXISTS \\"blobs_\${SG}\\" CASCADE;"
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
            _pg_import_binary() {
                local TABLE=\$1
                local PGBIN_FILE=\$2
                echo "Importing binary \$TABLE \$PGBIN_FILE ..."
                psql -v ON_ERROR_STOP=1 -c "\\\\COPY \\"\$TABLE\\" FROM STDIN WITH (FORMAT binary)" < "\$PGBIN_FILE"
            }
            export -f _pg_import_binary
            printf '%s\\0' postgres_blobs_\${SG}_*.pgbin | \\
                xargs -0 -P \$BLOB_WORKERS -n1 bash -c "set -e; _pg_import_binary \\"blobs_\${SG}\\" \\"\\\$1\\"" _
        fi

        echo "Creating blobs primary key for \$SG ..."
        \$PSQL -c "ALTER TABLE \\"blobs_\${SG}\\" ADD PRIMARY KEY (id);"
        \$PSQL -c "ANALYZE \\"blobs_\${SG}\\";"

        # --- AUTOCOMPLETE TABLE ---
        \$PSQL -c "DROP TABLE IF EXISTS \\"autocomplete_\${SG}\\" CASCADE;"
        \$PSQL -c "
            CREATE TABLE \\"autocomplete_\${SG}\\" (
                label TEXT NOT NULL
            ) WITH (fillfactor=100);
        "

        AUTOCOMPLETE_FILES=(autocomplete_\${SG}_*.tsv.gz)
        if [ -f "\${AUTOCOMPLETE_FILES[0]}" ]; then
            echo "Importing \${#AUTOCOMPLETE_FILES[@]} autocomplete files for \$SG..."
            printf '%s\\0' autocomplete_\${SG}_*.tsv.gz | \\
                xargs -0 -P \$NPROC -n1 bash -c "set -e; _pg_import \\"autocomplete_\${SG}\\" \\"\\\$1\\"" _
        fi

        echo "Creating autocomplete indexes for \$SG..."
        \$PSQL -c "CREATE INDEX \\"idx_autocomplete_\${SG}_trgm\\" ON \\"autocomplete_\${SG}\\" USING gin (label gin_trgm_ops);"
        \$PSQL -c "ANALYZE \\"autocomplete_\${SG}\\";"

        # --- MATERIALISED QUERIES TABLE ---
        \$PSQL -c "DROP TABLE IF EXISTS \\"materialised_queries_\${SG}\\" CASCADE;"
        \$PSQL -c "
            CREATE TABLE \\"materialised_queries_\${SG}\\" (
                query_id TEXT NOT NULL,
                row_number INT NOT NULL,
                data JSONB NOT NULL
            ) WITH (fillfactor=100);
        "

        MAT_QUERIES_FILES=(mat_queries_\${SG}_*.tsv.gz)
        if [ -f "\${MAT_QUERIES_FILES[0]}" ]; then
            echo "Importing \${#MAT_QUERIES_FILES[@]} materialised query files for \$SG..."
            printf '%s\\0' mat_queries_\${SG}_*.tsv.gz | \\
                xargs -0 -P \$NPROC -n1 bash -c "set -e; _pg_import \\"materialised_queries_\${SG}\\" \\"\\\$1\\"" _
        fi

        echo "Creating materialised query indexes for \$SG..."
        \$PSQL -c "CREATE INDEX \\"idx_mat_queries_\${SG}_query_id\\" ON \\"materialised_queries_\${SG}\\" USING btree (query_id);"
        \$PSQL -c "CREATE INDEX \\"idx_mat_queries_\${SG}_query_id_row\\" ON \\"materialised_queries_\${SG}\\" USING btree (query_id, row_number);"
        \$PSQL -c "ANALYZE \\"materialised_queries_\${SG}\\";"
    done

    mkdir -p postgres_external_done
    echo "Populated \${PGUSER}@\${PGHOST}:\${PGPORT}/\${PGDATABASE} at \$(date)" > postgres_external_done/status.txt
    echo "Subgraphs: \${SUBGRAPHS[*]}" >> postgres_external_done/status.txt
    echo "=== Done ==="
    echo "External database \${PGDATABASE} on \${PGHOST} has been populated."
    """
}
