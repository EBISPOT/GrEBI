process assign_ids {
    cache "lenient"
    memory { 32.GB + 128.GB * (task.attempt-1) }
    time { 1.hour + 8.hour * (task.attempt-1) }
    errorStrategy { task.exitStatus in 137..140 ? 'retry' : 'terminate' }
    maxRetries 5

    input:
    tuple val(subgraph), val(datasource_name), path(nodes_jsonl), path(groups_txt), val(identifier_props), val(type_superclasses)

    output:
    tuple val(subgraph), val(datasource_name), path("nodes_with_ids.sorted.jsonl.gz")

    script:
    """
    #!/usr/bin/env bash
    set -Eeuo pipefail
    cat ${nodes_jsonl} \
        | grebi_assign_ids \
            --identifier-properties ${identifier_props.iterator().join(",")} \
            --groups-txt ${groups_txt} \
        | grebi_superclasses2types \
            --type-superclasses ${type_superclasses.iterator().join(",")} \
            --groups-txt ${groups_txt} \
        > nodes_with_ids.jsonl
    LC_ALL=C sort -o nodes_with_ids.sorted.jsonl nodes_with_ids.jsonl
    rm -f nodes_with_ids.jsonl
    pigz --fast nodes_with_ids.sorted.jsonl
    """
}
