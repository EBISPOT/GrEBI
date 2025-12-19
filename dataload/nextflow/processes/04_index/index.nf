process index {
    cache "lenient"
    memory "4 GB" 
    time "8h"

    input:
    val(merged_filenames)
    val(subgraph)

    output:
    path("entity_metadata.jsonl"), emit: entity_metadata_jsonl
    path("graph_metadata.json"), emit: graph_metadata_json
    path("names.txt"), emit: names_txt
    path("ids_${subgraph}.txt"), emit: ids_txt

    script:
    """
    #!/usr/bin/env bash
    set -Eeuo pipefail
    cat ${merged_filenames.iterator().join(" ")} \
        | grebi_index \
        --subgraph-name ${subgraph} \
        --out-entity-metadata-jsonl-path entity_metadata.jsonl \
        --out-graph-metadata-json-path graph_metadata.json \
        --out-names-txt names.txt \
        --out-ids-txt ids_${subgraph}.txt
    """
}
