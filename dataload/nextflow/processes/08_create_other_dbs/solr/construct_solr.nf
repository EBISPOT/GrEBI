process construct_solr {
    cache "lenient"
    memory "4 GB"
    time "8h"
    cpus "4"

    input:
    path(cores)

    output:
    path("solr")

    script:
    """
    #!/usr/bin/env bash
    set -Eeuo pipefail
    mkdir -p solr
    cp -f /opt/grebi_dataload/08_create_other_dbs/solr/solr_config_template/*.xml solr/
    cp -f /opt/grebi_dataload/08_create_other_dbs/solr/solr_config_template/*.cfg solr/
    for core in ${cores}; do
        ln -s \$(readlink -f "\$core") solr/
    done
    """
}
