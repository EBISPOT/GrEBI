process create_solr_autocomplete_core {
    cache "lenient"
    memory "4 GB" 
    time "4h"
    cpus "4"

    input:
    path(names_txt)
    val(subgraph)
    val(solr_mem)

    output:
    path("solr/data/grebi_autocomplete_${subgraph}")

    script:
    """
    #!/usr/bin/env bash
    set -Eeuo pipefail
    mkdir -p solr/data solr/logs
    python3 /opt/grebi_dataload/08_create_other_dbs/solr/make_solr_autocomplete_config.py \
        --subgraph-name ${subgraph} \
        --in-template-config-dir /opt/grebi_dataload/08_create_other_dbs/solr/solr_config_template \
        --out-config-dir ./solr/data
    python3 /opt/grebi_dataload/08_create_other_dbs/solr/solr_import.dockerpy \
        grebi_autocomplete_${subgraph} 8987 ${solr_mem}
    """
}
