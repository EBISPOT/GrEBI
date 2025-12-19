process package_solr {
    cache "lenient"
    memory "4 GB" 
    time "8h"
    cpus "8"

    input: 
    path(cores)
    val(subgraph)
    val(out_dir)

    publishDir "${out_dir}", overwrite: true

    output:
    path("${subgraph}_solr.tgz")

    script:
    """
    #!/usr/bin/env bash
    set -Eeuo pipefail
    cp -f /opt/grebi_dataload/08_create_other_dbs/solr/solr_config_template/*.xml .
    cp -f /opt/grebi_dataload/08_create_other_dbs/solr/solr_config_template/*.cfg .
    tar -chf ${subgraph}_solr.tgz --transform 's,^,solr/,' --use-compress-program="pigz --fast" \
	*.xml *.cfg ${cores}
    """
}
