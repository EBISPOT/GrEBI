process index {
    cache "lenient"
    memory "4 GB" 
    time "8h"

    input:
    tuple val(subgraph), val(merged_filenames), val(subgraph_config_json_path)

    output:
    tuple val(subgraph), path("entity_metadata.jsonl"), emit: entity_metadata_jsonl
    tuple val(subgraph), path("graph_metadata.json"), emit: graph_metadata_json
    tuple val(subgraph), path("names.txt"), emit: names_txt
    tuple val(subgraph), path("ids_${subgraph}.txt"), emit: ids_txt

    script:
    """
    #!/usr/bin/env bash
    set -Eeuo pipefail
    cat ${merged_filenames.iterator().join(" ")} \
        | grebi_index \
        --subgraph-name ${subgraph} \
        --subgraph-config-json-path ${subgraph_config_json_path} \
        --out-entity-metadata-jsonl-path entity_metadata.jsonl \
        --out-graph-metadata-json-path graph_metadata.json \
        --out-names-txt names.txt \
        --out-ids-txt ids_${subgraph}.txt
    """
}
