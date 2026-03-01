process create_postgres {
    cache "lenient"
    memory "8 GB"
    time "23h"
    cpus "4"

    input:
    path(edges_tsvs)
    path(schema_sqls)
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

    # Initialise a fresh PostgreSQL data directory
    initdb --username=\$PGUSER --auth=trust --no-locale --encoding=UTF8 -D \$PGDATA

    # Tune for bulk import
    cat >> \$PGDATA/postgresql.conf <<EOF
listen_addresses = ''
unix_socket_directories = '\$PWD'
shared_buffers = ${params.pg_shared_buffers}
work_mem = ${params.pg_work_mem}
maintenance_work_mem = ${params.pg_maintenance_work_mem}
max_wal_size = ${params.pg_max_wal_size}
wal_level = minimal
max_wal_senders = 0
fsync = off
synchronous_commit = off
full_page_writes = off
checkpoint_completion_target = 0.9
EOF

    # Start PostgreSQL locally (unix socket only)
    pg_ctl -D \$PGDATA -l \$PWD/pg_startup.log start -o "-p \$PGPORT -k \$PWD" || {
        echo "=== PostgreSQL startup log ===" >&2
        cat \$PWD/pg_startup.log >&2
        exit 1
    }

    # Wait for postgres to start
    for i in \$(seq 1 30); do
        if pg_isready -h \$PWD -p \$PGPORT -U \$PGUSER; then
            break
        fi
        sleep 1
    done

    createdb -h \$PWD -p \$PGPORT -U \$PGUSER grebi

    # Apply schema (use the first schema file - all should be identical for same subgraph)
    SCHEMA_FILE=\$(ls postgres_schema_${subgraph}_*.sql | head -1)
    psql -h \$PWD -p \$PGPORT -U \$PGUSER -d grebi -f "\$SCHEMA_FILE"

    # Import all TSV files
    # First drop indexes for faster import
    psql -h \$PWD -p \$PGPORT -U \$PGUSER -d grebi -c "
        DROP INDEX IF EXISTS idx_edges_${subgraph}_fromNodeId;
        DROP INDEX IF EXISTS idx_edges_${subgraph}_toNodeId;
        DROP INDEX IF EXISTS idx_edges_${subgraph}_type;
    "

    for TSV_FILE in postgres_edges_${subgraph}_*.tsv; do
        echo "Importing \$TSV_FILE ..."
        psql -h \$PWD -p \$PGPORT -U \$PGUSER -d grebi -c "\\COPY \\"edges_${subgraph}\\" FROM '\$TSV_FILE' WITH (FORMAT text)"
    done

    # Recreate indexes
    echo "Creating indexes..."
    psql -h \$PWD -p \$PGPORT -U \$PGUSER -d grebi -c "
        CREATE INDEX idx_edges_${subgraph}_fromNodeId ON \\"edges_${subgraph}\\" (\\"grebi:fromNodeId\\");
        CREATE INDEX idx_edges_${subgraph}_toNodeId ON \\"edges_${subgraph}\\" (\\"grebi:toNodeId\\");
        CREATE INDEX idx_edges_${subgraph}_type ON \\"edges_${subgraph}\\" (\\"grebi:type\\");
    "

    # Analyse for query planner
    psql -h \$PWD -p \$PGPORT -U \$PGUSER -d grebi -c "ANALYZE \\"edges_${subgraph}\\";"

    # Stop PostgreSQL cleanly
    pg_ctl -D \$PGDATA stop -m fast

    # Re-enable WAL for production use
    sed -i 's/^wal_level = minimal/wal_level = replica/' \$PGDATA/postgresql.conf
    sed -i 's/^fsync = off/fsync = on/' \$PGDATA/postgresql.conf
    sed -i 's/^synchronous_commit = off/synchronous_commit = on/' \$PGDATA/postgresql.conf
    sed -i 's/^full_page_writes = off/full_page_writes = on/' \$PGDATA/postgresql.conf
    # Update socket dir to /var/run/postgresql for deployment
    sed -i "s|unix_socket_directories = .*|unix_socket_directories = '/var/run/postgresql'|" \$PGDATA/postgresql.conf
    # Enable TCP connections for deployment
    sed -i "s/^listen_addresses = ''/listen_addresses = '*'/" \$PGDATA/postgresql.conf

    # Add pg_hba.conf entry for network connections
    echo "host all all 0.0.0.0/0 trust" >> \$PGDATA/pg_hba.conf

    echo "PostgreSQL data directory built: \$PGDATA"
    """
}
