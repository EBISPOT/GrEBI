process package_neo {
    cache "lenient"
    memory "4 GB" 
    time "8h"
    cpus "8"

    input: 
    path(neo4j_dir)
    val(subgraph)
    val(out_dir)

    publishDir "${out_dir}", overwrite: true

    output:
    path("${subgraph}_neo4j.tgz")

    script:
    """
    tar -chf ${subgraph}_neo4j.tgz --use-compress-program="pigz --fast" ${neo4j_dir}
    """
}
