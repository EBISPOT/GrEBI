process prepare_solr {
    cache "lenient"
    memory "4 GB" 
    time "1h"

    input:
    path(nodes_jsonl)
    path(edges_jsonl)
    val(subgraph)

    output:
    path("solr_nodes_${subgraph}_${task.index}.jsonl"), emit: nodes
    path("solr_edges_${subgraph}_${task.index}.jsonl"), emit: edges

    script:
    """
    #!/usr/bin/env bash
    set -Eeuo pipefail
    grebi_make_solr  \
      --in-nodes-jsonl ${nodes_jsonl} \
      --in-edges-jsonl ${edges_jsonl} \
      --out-nodes-jsonl-path solr_nodes_${subgraph}_${task.index}.jsonl \
      --out-edges-jsonl-path solr_edges_${subgraph}_${task.index}.jsonl
    """
}
