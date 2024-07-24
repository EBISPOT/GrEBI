
nextflow.enable.dsl=2

import groovy.json.JsonSlurper
jsonSlurper = new JsonSlurper()

params.tmp = "$GREBI_TMP"
params.home = "$GREBI_HOME"
params.config = "$GREBI_CONFIG"
params.timestamp = "$GREBI_TIMESTAMP"

workflow {

    subgraph_dirs = Channel.fromPath("${params.tmp}/*", type: 'dir')

    neo_nodes_files = Channel.fromPath("${params.tmp}/${params.config}/*/neo4j_csv/neo_nodes_*.csv").collect()
    neo_edges_files = Channel.fromPath("${params.tmp}/${params.config}/*/neo4j_csv/neo_edges_*.csv").collect()

    neo_db = create_neo(neo_nodes_files.collect() + neo_edges_files.collect())

    solr_tgz = package_solr( Channel.fromPath("${params.tmp}/${params.config}/*/solr_cores/*").collect())
    rocks_tgz = package_rocks( Channel.fromPath("${params.tmp}/${params.config}/*/*_rocksdb").collect())

    neo_tgz = package_neo(neo_db)

    copy_solr_to_ftp(solr_tgz)
    copy_neo_to_ftp(neo_tgz)
    copy_rocks_to_ftp(rocks_tgz)

    if(params.config == "ebi") {
        copy_neo_to_staging(neo_db)
    }
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

process package_neo {
    cache "lenient"
    memory "32 GB" 
    time "8h"
    cpus "32"

    publishDir "${params.tmp}/${params.config}", overwrite: true

    input: 
    path(neo4j)

    output:
    path("combined_neo4j.tgz")

    script:
    """
    tar -chf combined_neo4j.tgz --use-compress-program="pigz --fast" neo4j
    """
}

process package_rocks {
    cache "lenient"
    memory "32 GB" 
    time "8h"
    cpus "32"

    publishDir "${params.tmp}/${params.config}", overwrite: true

    input: 
    path(rocks_dbs)

    output:
    path("combined_rocksdb.tgz")

    script:
    """
    tar -chf combined_rocksdb.tgz --use-compress-program="pigz --fast" ${rocks_dbs}
    """
}

process package_solr {
    cache "lenient"
    memory "32 GB" 
    time "8h"
    cpus "32"

    publishDir "${params.tmp}/${params.config}", overwrite: true

    input: 
    path(cores)

    output:
    path("combined_solr.tgz")

    script:
    """
    #!/usr/bin/env bash
    set -Eeuo pipefail
    cp -f ${params.home}/06_prepare_db_import/solr_config_template/*.xml .
    cp -f ${params.home}/06_prepare_db_import/solr_config_template/*.cfg .
    tar -chf combined_solr.tgz --transform 's,^,solr/,' --use-compress-program="pigz --fast" \
	*.xml *.cfg ${cores}
    """
}

process copy_neo_to_ftp {
    
    cache "lenient"
    memory "4 GB" 
    time "8h"
    queue "datamover"

    input: 
    path("combined_neo4j.tgz")

    script:
    """
    #!/usr/bin/env bash
    set -Eeuo pipefail
    mkdir -p /nfs/ftp/public/databases/spot/kg/${params.config}/${params.timestamp.trim()}
    cp -f combined_neo4j.tgz /nfs/ftp/public/databases/spot/kg/${params.config}/${params.timestamp.trim()}/
    """
}

process copy_solr_to_ftp {
    
    cache "lenient"
    memory "4 GB" 
    time "8h"
    queue "datamover"

    input: 
    path("combined_solr.tgz")

    script:
    """
    #!/usr/bin/env bash
    set -Eeuo pipefail
    mkdir -p /nfs/ftp/public/databases/spot/kg/${params.config}/${params.timestamp.trim()}
    cp -f combined_solr.tgz /nfs/ftp/public/databases/spot/kg/${params.config}/${params.timestamp.trim()}/
    """
}

process copy_rocks_to_ftp {
    
    cache "lenient"
    memory "4 GB" 
    time "8h"
    queue "datamover"

    input: 
    path("combined_rocksdb.tgz")

    script:
    """
    #!/usr/bin/env bash
    set -Eeuo pipefail
    mkdir -p /nfs/ftp/public/databases/spot/kg/${params.config}/${params.timestamp.trim()}
    cp -f combined_rocksdb.tgz /nfs/ftp/public/databases/spot/kg/${params.config}/${params.timestamp.trim()}/
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

def parseJson(json) {
    return new JsonSlurper().parseText(json)
}

def basename(filename) {
    return new File(filename).name
}