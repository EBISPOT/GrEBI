
nextflow.enable.dsl=2

import groovy.json.JsonSlurper
jsonSlurper = new JsonSlurper()

params.tmp = "$GREBI_TMP"
params.home = "$GREBI_HOME"
params.config = "$GREBI_CONFIG"

workflow {

    rocks_db = create_rocks(Channel.fromPath("${params.tmp}/*/*.jsonl").collect())

    subgraph_dirs = Channel.fromPath("${params.tmp}/*", type: 'dir')

    nodes_per_subgraph = subgraph_dirs
        | flatMap { d -> (d + "/nodes/").listFiles().collect { f -> tuple(d.name, f) } }

    edges_per_subgraph = subgraph_dirs
        | flatMap { d -> (d + "/edges/").listFiles().collect { f -> tuple(d.name, f) } }

    neo_input_dir = prepare_neo(
        subgraph_dirs.map { d -> "${d}/summary.json" }.collect(),
        nodes_per_subgraph.map { t -> t[1] },
        edges_per_subgraph.map { t -> t[1] }
    )
    neo_db = create_neo(prepare_neo.out.nodes.collect() + prepare_neo.out.edges.collect())

    solr_inputs = prepare_solr(nodes_per_subgraph, edges_per_subgraph)
        | map { t -> tuple(t[0], t) }
        | groupTuple

    solr_nodes_per_subgraph = solr_inputs | map { t -> tuple(t[0], t[1].collect { u -> u[1] }) }
    solr_edges_per_subgraph = solr_inputs | map { t -> tuple(t[0], t[1].collect { u -> u[2] }) }

    solr_nodes_cores = create_solr_nodes_core(solr_nodes_per_subgraph)
    solr_edges_cores = create_solr_edges_core(solr_edges_per_subgraph)

    solr_autocomplete_cores = create_solr_autocomplete_core(
        subgraph_dirs.map { d -> tuple(d.name, "${d}/names.txt") }
    )

    solr_tgz = package_solr(
        solr_nodes_cores.collect(),
        solr_edges_cores.collect(),
        solr_autocomplete_cores.collect())

    neo_tgz = package_neo(neo_db)
    rocks_tgz = package_rocks(rocks_db)

    date = get_date()
    copy_solr_to_ftp(solr_tgz, date)
    copy_neo_to_ftp(neo_tgz, date)
    copy_rocks_to_ftp(rocks_tgz, date)

    if(params.config == "ebi_full_monarch") {
        copy_solr_to_staging(solr_nodes_cores.collect(), solr_edges_cores.collect(), solr_autocomplete_cores.collect())
        copy_neo_to_staging(neo_db)
        copy_rocksdb_to_staging(rocks_db)
    }
}

process create_rocks {
    cache "lenient"
    memory "800 GB" 
    time "23h"
    cpus "8"
    errorStrategy 'retry'
    maxRetries 10

    input:
    val(materialised)

    output:
    path("rocksdb")

    script:
    """
    #!/usr/bin/env bash
    set -Eeuo pipefail
    cat ${materialised.iterator().join(" ")} \
        | ${params.home}/target/release/grebi_make_rocks \
            --rocksdb-path /dev/shm/rocksdb && \
    mv /dev/shm/rocksdb .
    """
}

process prepare_neo {
    cache "lenient"
    memory "4 GB" 
    time "1h"

    input:
    val(summary_jsons)
    path(nodes_jsonl)
    path(edges_jsonl)

    output:
    path("neo_nodes_${task.index}.csv"), emit: nodes
    path("neo_edges_${task.index}.csv"), emit: edges

    script:
    """
    #!/usr/bin/env bash
    set -Eeuo pipefail
    ${params.home}/target/release/grebi_make_csv \
      --in-summary-jsons ${summary_jsons.join(",")} \
      --in-nodes-jsonl ${nodes_jsonl} \
      --in-edges-jsonl ${edges_jsonl} \
      --out-nodes-csv-path neo_nodes_${task.index}.csv \
      --out-edges-csv-path neo_edges_${task.index}.csv
    """
}

process prepare_solr {
    cache "lenient"
    memory "4 GB" 
    time "1h"

    input:
    tuple val(subgraph_name), val(nodes_jsonl)
    tuple val(subgraph_name_), val(edges_jsonl)

    output:
    tuple val(subgraph_name), path("solr_nodes_${task.index}.jsonl"), path("solr_edges_${task.index}.jsonl")

    script:
    """
    #!/usr/bin/env bash
    set -Eeuo pipefail
    ${params.home}/target/release/grebi_make_solr  \
      --in-nodes-jsonl ${nodes_jsonl} \
      --in-edges-jsonl ${edges_jsonl} \
      --out-nodes-jsonl-path solr_nodes_${task.index}.jsonl \
      --out-edges-jsonl-path solr_edges_${task.index}.jsonl
    """
}

process create_neo {
    cache "lenient"
    memory "50 GB" 
    time "8h"
    cpus "32"

    input:
    path(neo_inputs)

    output:
    path("neo4j")

    script:
    """
    #!/usr/bin/env bash
    set -Eeuo pipefail
    PYTHONUNBUFFERED=true python3 ${params.home}/07_create_db/neo4j/neo4j_import.slurm.py \
        --in-csv-path . \
        --out-db-path neo4j
    """
}

process create_solr_nodes_core {
    cache "lenient"
    memory "150 GB" 
    time "23h"
    cpus "32"

    input:
    tuple val(subgraph_name), path(solr_inputs)

    output:
    path("solr/data/grebi_nodes_${subgraph_name}")

    script:
    """
    #!/usr/bin/env bash
    set -Eeuo pipefail
    python3 ${params.home}/06_prepare_db_import/make_solr_config.py \
        --subgraph-name ${subgraph_name} \
        --in-summary-json ${params.tmp}/${subgraph_name}/summary.json \
        --in-template-config-dir ${params.home}/06_prepare_db_import/solr_config_template \
        --out-config-dir solr_config
    python3 ${params.home}/07_create_db/solr/solr_import.slurm.py \
        --solr-config solr_config --core grebi_nodes_${subgraph_name} --in-data . --out-path solr --port 8985
    """
}

process create_solr_edges_core {
    cache "lenient"
    memory "150 GB" 
    time "23h"
    cpus "32"

    input:
    tuple val(subgraph_name), path(solr_inputs)

    output:
    path("solr/data/grebi_edges_${subgraph_name}")

    script:
    """
    #!/usr/bin/env bash
    set -Eeuo pipefail
    python3 ${params.home}/06_prepare_db_import/make_solr_config.py \
        --subgraph-name ${subgraph_name} \
        --in-summary-json ${params.tmp}/${subgraph_name}/summary.json \
        --in-template-config-dir ${params.home}/06_prepare_db_import/solr_config_template \
        --out-config-dir solr_config
    python3 ${params.home}/07_create_db/solr/solr_import.slurm.py \
        --solr-config solr_config --core grebi_edges_${subgraph_name} --in-data . --out-path solr --port 8986
    """
}

process create_solr_autocomplete_core {
    cache "lenient"
    memory "150 GB" 
    time "4h"
    cpus "4"

    input:
    tuple val(subgraph_name), path(names_txt)

    output:
    path("solr/data/grebi_autocomplete_${subgraph_name}")

    script:
    """
    #!/usr/bin/env bash
    set -Eeuo pipefail
    python3 ${params.home}/06_prepare_db_import/make_solr_autocomplete_config.py \
        --subgraph-name ${subgraph_name} \
        --in-template-config-dir ${params.home}/06_prepare_db_import/solr_config_template \
        --out-config-dir solr_config
    python3 ${params.home}/07_create_db/solr/solr_import.slurm.py \
        --solr-config solr_config --core grebi_autocomplete_${subgraph_name} --in-data . --in-names-txt ${names_txt} --out-path solr --port 8987
    """
}

process package_neo {
    cache "lenient"
    memory "32 GB" 
    time "8h"
    cpus "32"

    publishDir "${params.home}/release/${params.config}", overwrite: true

    input: 
    path(neo4j)

    output:
    path("neo4j.tgz")

    script:
    """
    tar -chf neo4j.tgz --use-compress-program="pigz --fast" neo4j
    """
}

process package_rocks {
    cache "lenient"
    memory "32 GB" 
    time "8h"
    cpus "32"

    publishDir "${params.home}/release/${params.config}", overwrite: true

    input: 
    path(rocks_db)

    output:
    path("rocksdb.tgz")

    script:
    """
    tar -chf rocksdb.tgz --use-compress-program="pigz --fast" ${rocks_db}
    """
}

process package_solr {
    cache "lenient"
    memory "32 GB" 
    time "8h"
    cpus "32"

    publishDir "${params.home}/release/${params.config}", overwrite: true

    input: 
    path(solr_nodes_cores)
    path(solr_edges_cores)
    path(solr_autocomplete_cores)

    output:
    path("solr.tgz")

    script:
    """
    #!/usr/bin/env bash
    set -Eeuo pipefail
    cp -f ${params.home}/06_prepare_db_import/solr_config_template/*.xml .
    cp -f ${params.home}/06_prepare_db_import/solr_config_template/*.cfg .
    tar -chf solr.tgz --transform 's,^,solr/,' --use-compress-program="pigz --fast" \
	*.xml *.cfg ${solr_nodes_cores} ${solr_edges_cores} ${solr_autocomplete_cores}
    """
}

process get_date {

    cache "lenient"
    memory "1 GB"
    time "1h"
    
    output:
    stdout

    script:
    """
    #!/usr/bin/env bash
    set -Eeuo pipefail
    date +%Y_%m_%d__%H_%M
    """
}

process copy_neo_to_ftp {
    
    cache "lenient"
    memory "4 GB" 
    time "8h"
    queue "datamover"

    input: 
    path("neo4j.tgz")
    val(date)

    script:
    """
    #!/usr/bin/env bash
    set -Eeuo pipefail
    mkdir -p /nfs/ftp/public/databases/spot/kg/${params.config}/${date.trim()}
    cp -f neo4j.tgz /nfs/ftp/public/databases/spot/kg/${params.config}/${date.trim()}/
    """
}

process copy_solr_to_ftp {
    
    cache "lenient"
    memory "4 GB" 
    time "8h"
    queue "datamover"

    input: 
    path("solr.tgz")
    val(date)

    script:
    """
    #!/usr/bin/env bash
    set -Eeuo pipefail
    mkdir -p /nfs/ftp/public/databases/spot/kg/${params.config}/${date.trim()}
    cp -f solr.tgz /nfs/ftp/public/databases/spot/kg/${params.config}/${date.trim()}/
    """
}

process copy_rocks_to_ftp {
    
    cache "lenient"
    memory "4 GB" 
    time "8h"
    queue "datamover"

    input: 
    path("rocksdb.tgz")
    val(date)

    script:
    """
    #!/usr/bin/env bash
    set -Eeuo pipefail
    mkdir -p /nfs/ftp/public/databases/spot/kg/${params.config}/${date.trim()}
    cp -f rocksdb.tgz /nfs/ftp/public/databases/spot/kg/${params.config}/${date.trim()}/
    """
}

process copy_neo_to_staging {
    cache "lenient"
    memory "4 GB" 
    time "8h"
    queue "datamover"

    input: 
    path("neo4j")

    script:
    """
    #!/usr/bin/env bash
    set -Eeuo pipefail
    rm -rf /nfs/public/rw/ontoapps/grebi/staging/neo4j
    cp -LR neo4j /nfs/public/rw/ontoapps/grebi/staging/neo4j
    """
}

process copy_solr_to_staging {
    cache "lenient"
    memory "4 GB" 
    time "8h"
    queue "datamover"

    input: 
    path(solr_nodes_cores)
    path(solr_edges_cores)
    path(solr_autocomplete_cores)

    script:
    """
    #!/usr/bin/env bash
    set -Eeuo pipefail
    rm -rf /nfs/public/rw/ontoapps/grebi/staging/solr && mkdir /nfs/public/rw/ontoapps/grebi/staging/solr
    cp -f ${params.home}/06_prepare_db_import/solr_config_template/*.xml .
    cp -f ${params.home}/06_prepare_db_import/solr_config_template/*.cfg .
    cp -LR * /nfs/public/rw/ontoapps/grebi/staging/solr/
    """
}

process copy_rocksdb_to_staging {
    cache "lenient"
    memory "4 GB" 
    time "8h"
    queue "datamover"

    publishDir "/nfs/public/rw/ontoapps/grebi/staging/rocksdb", mode: 'copy', overwrite: true

    input: 
    path("rocksdb")

    script:
    """
    #!/usr/bin/env bash
    set -Eeuo pipefail
    rm -rf /nfs/public/rw/ontoapps/grebi/staging/rocksdb
    cp -LR rocksdb /nfs/public/rw/ontoapps/grebi/staging/rocksdb
    """
}

def parseJson(json) {
    return new JsonSlurper().parseText(json)
}

def basename(filename) {
    return new File(filename).name
}
