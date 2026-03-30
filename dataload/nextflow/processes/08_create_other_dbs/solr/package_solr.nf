process package_solr {
    cache "lenient"
    memory "4 GB" 
    time "8h"
    cpus "4"

    input: 
    path(solr_dir)
    val(out_dir)

    publishDir "${out_dir}", overwrite: true

    output:
    path("solr.tgz")

    script:
    """
    #!/usr/bin/env bash
    set -Eeuo pipefail
    tar -chf solr.tgz --use-compress-program="pigz --fast" ${solr_dir}
    """
}
