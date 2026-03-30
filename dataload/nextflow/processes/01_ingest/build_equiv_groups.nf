def buildAddEquivGroupArgs(equivGroups) {
    def res = ""
    equivGroups.each { arg -> res += "--add-group ${arg.iterator().join(",")} " }
    return res
}

process build_equiv_groups {
    cache "lenient"
    memory '4 GB'
    time '23h'

    input:
    tuple val(subgraph), path(identifiers_tsv), val(additional_equivalence_groups)

    output:
    tuple val(subgraph), path("groups.txt")

    script:
    """
    #!/usr/bin/env bash
    set -Eeuo pipefail
    cat ${identifiers_tsv} \
        | grebi_identifiers2groups \
            ${buildAddEquivGroupArgs(additional_equivalence_groups)} \
        > groups.txt
    """
}
