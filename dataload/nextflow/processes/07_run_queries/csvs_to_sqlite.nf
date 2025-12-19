process csvs_to_sqlite {
    cache "lenient"
    memory "64 GB" 
    time "12h"
    cpus "8"

    input:
    path(csvs)
    val(out_dir)

    publishDir "${out_dir}", overwrite: true

    output:
    path("materialised_queries.sqlite3.gz")

    script:
    """
    #!/usr/bin/env bash
    set -Eeuo pipefail
    PYTHONUNBUFFERED=true python3 /opt/grebi_dataload/07_run_queries/csvs_to_sqlite.py --out-sqlite-path materialised_queries.sqlite3
    pigz --best materialised_queries.sqlite3
    """
}
