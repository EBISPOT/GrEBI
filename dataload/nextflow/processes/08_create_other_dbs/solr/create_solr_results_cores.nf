process create_solr_results_cores {
    cache "lenient"
    memory "4 GB" 
    time "4h"
    cpus "4"

    input:
    tuple val(subgraph), path(results_jsonl)
    val(solr_mem)

    output:
    tuple val(subgraph), path("solr/data/grebi_results__${subgraph}__${results_jsonl.simpleName}")

    script:
    """
    #!/usr/bin/env bash
    set -Eeuo pipefail
    mkdir -p solr/data solr/logs
    python3 /opt/grebi_dataload/08_create_other_dbs/solr/make_solr_results_config.py \
        --subgraph-name ${subgraph} \
        --query-id ${results_jsonl.simpleName} \
        --in-template-config-dir /opt/grebi_dataload/08_create_other_dbs/solr/solr_config_template \
        --out-config-dir ./solr/data
    python3 /opt/grebi_dataload/08_create_other_dbs/solr/solr_import.dockerpy \
        grebi_results__${subgraph}__${results_jsonl.simpleName} 8987 ${solr_mem}
    """
}
