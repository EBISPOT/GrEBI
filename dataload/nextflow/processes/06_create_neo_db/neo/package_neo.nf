process package_neo {
    cache "lenient"
    memory "4 GB" 
    time "8h"
    cpus "4"

    input: 
    tuple val(subgraph), path(neo4j_dir)
    val(out_dir)

    publishDir "${out_dir}", overwrite: true

    output:
    tuple val(subgraph), path("${subgraph}_neo4j.tgz")

    script:
    """
    tar -chf ${subgraph}_neo4j.tgz --use-compress-program="pigz --fast" ${neo4j_dir}
    """
}
