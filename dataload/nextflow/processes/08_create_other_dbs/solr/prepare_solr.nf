process prepare_solr {
    cache "lenient"
    memory "4 GB" 
    time "1h"

    input:
    tuple val(subgraph), path(nodes_jsonl)

    output:
    tuple val(subgraph), path("solr_nodes_${subgraph}_${task.index}.jsonl"), emit: nodes

    script:
    """
    #!/usr/bin/env bash
    set -Eeuo pipefail
    grebi_make_solr  \
      --in-nodes-jsonl ${nodes_jsonl} \
      --out-nodes-jsonl-path solr_nodes_${subgraph}_${task.index}.jsonl
    """
}
