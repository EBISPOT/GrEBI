def buildMergeArgs(assigned) {
    def res = ""
    assigned.each { a ->
        res += "${a[0]}:${a[1]} "
    }
    return res
}

process merge_ingests {
    cache "lenient"
    memory "4 GB" 
    time "8h"

    input:
    val(assigned)
    val(exclude_props)
    val(prioritise_datasources)
    val(bytes_per_merged_file)
    val(subgraph)

    output:
    path('merged.jsonl.*')

    script:
    """
    #!/usr/bin/env bash
    set -Eeuo pipefail
    grebi_merge \
        --exclude-props ${exclude_props.iterator().join(",")} \
        --prioritise-datasources ${prioritise_datasources.iterator().join(",")} \
        --annotate-subgraph-name ${subgraph} \
        ${buildMergeArgs(assigned)} \
        | split -a 6 -d -C ${bytes_per_merged_file} - merged.jsonl.
    """
}
