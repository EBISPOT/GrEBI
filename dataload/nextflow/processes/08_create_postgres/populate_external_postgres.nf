process populate_external_postgres {
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
    path("postgres_external_done")

    script:
    """
    python3 ${projectDir}/processes/08_create_postgres/populate_external_postgres.py
    """
}
