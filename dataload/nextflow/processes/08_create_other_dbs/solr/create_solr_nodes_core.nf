process create_solr_nodes_core {
    cache "lenient"
    memory "4 GB" 
    time "23h"
    cpus "4"

    input:
    path(solr_inputs)
    path(names_txt)
    path(graph_metadata_json)
    val(subgraph)
    val(solr_mem)

    output:
    path("solr/data/grebi_nodes_${subgraph}")

    script:
    """
    #!/usr/bin/env bash
    set -Eeuo pipefail
    mkdir -p solr/data solr/logs
    python3 /opt/grebi_dataload/08_create_other_dbs/solr/make_solr_config.py \
        --subgraph-name ${subgraph} \
        --in-graph-metadata-json ${graph_metadata_json} \
        --in-template-config-dir /opt/grebi_dataload/08_create_other_dbs/solr/solr_config_template \
        --out-config-dir ./solr/data
    python3 /opt/grebi_dataload/08_create_other_dbs/solr/solr_import.dockerpy \
        grebi_nodes_${subgraph} 8985 ${solr_mem}
    """
}
