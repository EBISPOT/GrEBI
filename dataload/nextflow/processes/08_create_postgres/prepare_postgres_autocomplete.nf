process prepare_postgres_autocomplete {
    cache "lenient"
    memory "4 GB"
    time "2h"

    input:
    tuple val(subgraph), path(names_txt)

    output:
    tuple val(subgraph), path("autocomplete_${subgraph}_0.pgbin"), emit: autocomplete_pgbin

    script:
    """
    #!/usr/bin/env bash
    set -Eeuo pipefail
    # names.txt is null-delimited labels; convert to one-label-per-line, deduplicate, write pgbin
    tr '\\0' '\\n' < ${names_txt} | sort -u | grebi_make_postgres_autocomplete > autocomplete_${subgraph}_0.pgbin
    """
}
