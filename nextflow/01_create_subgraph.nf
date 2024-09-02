
nextflow.enable.dsl=2

import groovy.json.JsonSlurper
jsonSlurper = new JsonSlurper()

params.tmp = "$GREBI_TMP"
params.home = "$GREBI_HOME"
params.config = "$GREBI_CONFIG"
params.subgraph = "$GREBI_SUBGRAPH"
params.timestamp = "$GREBI_TIMESTAMP"
params.is_ebi = "$GREBI_IS_EBI"

workflow {

    config = (new JsonSlurper().parse(new File(params.home, 'configs/subgraph_configs/' + params.subgraph + '.json')))

    files_listing = prepare() | splitText | map { row -> parseJson(row) }

    ingest(files_listing, Channel.value(config.identifier_props))
    groups_txt = build_equiv_groups(ingest.out.identifiers.collect(), Channel.value(config.additional_equivalence_groups))
    assigned = assign_ids(ingest.out.nodes, groups_txt, Channel.value(config.identifier_props), Channel.value(config.type_superclasses)).collect(flat: false)

    merged = merge_ingests(
        assigned,
        Channel.value(config.exclude_props),
        Channel.value(config.bytes_per_merged_file))

    indexed = index(merged.collect())
    materialise(merged.flatten(), indexed.metadata_jsonl, Channel.value(config.exclude_edges + config.identifier_props), Channel.value(config.exclude_self_referential_edges + config.identifier_props), groups_txt)
    merge_summary_jsons(indexed.prop_summary_json.collect() + materialise.out.edge_summary.collect())

    materialised_nodes_and_edges = materialise.out.nodes.collect() + materialise.out.edges.collect()

    rocks_db = create_rocks(materialised_nodes_and_edges)

    neo_input_dir = prepare_neo(indexed.prop_summary_json, materialise.out.nodes, materialise.out.edges)

    ids_csv = create_neo_ids_csv(indexed.ids_txt)
    neo_db = create_neo(
        prepare_neo.out.nodes.collect() +
        prepare_neo.out.edges.collect() +
        prepare_neo.out.id_edges.collect() +
	ids_csv.collect()
)

    solr_inputs = prepare_solr(materialise.out.nodes, materialise.out.edges)
    solr_nodes_core = create_solr_nodes_core(prepare_solr.out.nodes.collect(), indexed.names_txt)
    solr_edges_core = create_solr_edges_core(prepare_solr.out.edges.collect(), indexed.names_txt)
    solr_autocomplete_core = create_solr_autocomplete_core(indexed.names_txt)

    solr_tgz = package_solr(solr_nodes_core, solr_edges_core, solr_autocomplete_core)
    neo_tgz = package_neo(neo_db)
    rocks_tgz = package_rocks(rocks_db)

    if(params.is_ebi == "true") {
    copy_summary_to_ftp(merge_summary_jsons.out)
    copy_solr_to_ftp(solr_tgz)
    copy_neo_to_ftp(neo_tgz)
    copy_rocks_to_ftp(rocks_tgz)

    if(params.config == "ebi") {
        copy_summary_to_staging(merge_summary_jsons.out)
        copy_solr_config_to_staging()
        copy_solr_cores_to_staging(solr_nodes_core.concat(solr_edges_core).concat(solr_autocomplete_core))
        copy_rocksdb_to_staging(rocks_db)
    }
    }
}

process prepare {
    cache "lenient"
    memory "4 GB"
    time "1h"

    output:
    path "datasource_files.jsonl"

    script: 
    """
    PYTHONUNBUFFERED=true python3 ${params.home}/scripts/dataload_00_prepare.py
    """
}

process ingest {
    cache "lenient"
    memory { 4.GB + 32.GB * (task.attempt-1) }
    time { 1.hour + 8.hour * (task.attempt-1) }
    errorStrategy { task.exitStatus in 137..140 ? 'retry' : 'terminate' }
    maxRetries 5
    
    input:
    val(file_listing)
    val(identifier_props)

    output:
    tuple val(file_listing.datasource.name), path("nodes_${task.index}.jsonl.gz"), emit: nodes
    path("identifiers_${task.index}.tsv"), emit: identifiers

    script:
    """
    #!/usr/bin/env bash
    set -Eeuo pipefail
    ${getStdinCommand(file_listing.ingest, file_listing.filename)} \
        ${getIngestCommand(file_listing.ingest.ingest_script)} \
            --datasource-name ${file_listing.datasource.name} \
            --filename "${file_listing.filename}" \
            ${buildIngestArgs(file_listing.ingest.ingest_args)} \
        | ${params.home}/target/release/grebi_normalise_prefixes ${params.home}/prefix_maps/prefix_map_normalise.json \
        | tee >(${params.home}/target/release/grebi_extract_identifiers \
                --identifier-properties ${identifier_props.iterator().join(",")} \
                    > identifiers_${task.index}.tsv) \
        | pigz --fast > nodes_${task.index}.jsonl.gz
    """
}

process build_equiv_groups {
    cache "lenient"
    memory '64 GB'
    time '23h'

    input:
    path(identifiers_tsv)
    val(additional_equivalence_groups)

    output:
    path "groups.txt"

    script:
    """
    #!/usr/bin/env bash
    set -Eeuo pipefail
    cat ${identifiers_tsv} \
        | ${params.home}/target/release/grebi_identifiers2groups \
            ${buildAddEquivGroupArgs(additional_equivalence_groups)} \
        > groups.txt
    """
}

process assign_ids {
    cache "lenient"
    memory { 32.GB + 32.GB * (task.attempt-1) }
    time { 1.hour + 8.hour * (task.attempt-1) }
    errorStrategy { task.exitStatus in 137..140 ? 'retry' : 'terminate' }
    maxRetries 5

    input:
    tuple(val(datasource_name), path(nodes_jsonl))
    path groups_txt
    val(identifier_props)
    val(type_superclasses)

    output:
    tuple(val(datasource_name), path("nodes_with_ids.sorted.jsonl.gz"))

    script:
    """
    #!/usr/bin/env bash
    set -Eeuo pipefail
    zcat ${nodes_jsonl} \
        | ${params.home}/target/release/grebi_assign_ids \
            --identifier-properties ${identifier_props.iterator().join(",")} \
            --groups-txt ${groups_txt} \
        | ${params.home}/target/release/grebi_superclasses2types \
            --type-superclasses ${type_superclasses.iterator().join(",")} \
            --groups-txt ${groups_txt} \
        > nodes_with_ids.jsonl
    LC_ALL=C sort -o nodes_with_ids.sorted.jsonl nodes_with_ids.jsonl
    rm -f nodes_with_ids.jsonl
    pigz --fast nodes_with_ids.sorted.jsonl
    """
}

process merge_ingests {
    cache "lenient"
    memory "16 GB" 
    time "8h"

    input:
    val(assigned)
    val(exclude_props)
    val(bytes_per_merged_file)

    output:
    path('merged.jsonl.*')

    script:
    """
    #!/usr/bin/env bash
    set -Eeuo pipefail
    ${params.home}/target/release/grebi_merge \
        --exclude-props ${exclude_props.iterator().join(",")} \
        --annotate-subgraph-name ${params.subgraph} \
        ${buildMergeArgs(assigned)} \
        | split -a 6 -d -C ${bytes_per_merged_file} - merged.jsonl.
    """
}

process index {
    cache "lenient"
    memory "64 GB" 
    time "8h"

    publishDir "${params.tmp}/${params.config}/${params.subgraph}", overwrite: true

    input:
    val(merged_filenames)

    output:
    path("metadata.jsonl"), emit: metadata_jsonl
    path("prop_summary.json"), emit: prop_summary_json
    path("names.txt"), emit: names_txt
    path("ids_${params.subgraph}.txt"), emit: ids_txt

    script:
    """
    #!/usr/bin/env bash
    set -Eeuo pipefail
    cat ${merged_filenames.iterator().join(" ")} \
        | ${params.home}/target/release/grebi_index \
        --subgraph-name ${params.subgraph} \
        --out-metadata-jsonl-path metadata.jsonl \
        --out-summary-json-path prop_summary.json \
        --out-names-txt names.txt \
        --out-ids-txt ids_${params.subgraph}.txt
    """
}

process materialise {
    cache "lenient"
    memory "64 GB"
    time "8h"
    //time { 1.hour + 8.hour * (task.attempt-1) }
    //errorStrategy { task.exitStatus in 137..140 ? 'retry' : 'terminate' }
    //maxRetries 5

    input:
    path(merged_filename)
    path(metadata_jsonl)
    val(exclude)
    val(exclude_self_referential)
    path(groups_txt)

    output:
    path("materialised_nodes_${task.index}.jsonl"), emit: nodes
    path("materialised_edges_${task.index}.jsonl"), emit: edges
    path("edge_summary_${task.index}.json"), emit: edge_summary

    script:
    """
    #!/usr/bin/env bash
    set -Eeuo pipefail
    cat ${merged_filename} \
        | ${params.home}/target/release/grebi_materialise \
          --in-metadata-jsonl ${metadata_jsonl} \
          --groups-txt ${groups_txt} \
          --out-edges-jsonl materialised_edges_${task.index}.jsonl \
          --out-edge-summary-json edge_summary_${task.index}.json \
          --exclude ${exclude.iterator().join(",")} \
          --exclude-self-referential ${exclude_self_referential.iterator().join(",")} \
        > materialised_nodes_${task.index}.jsonl
    """
}

process merge_summary_jsons {
    cache "lenient"
    memory "8 GB"
    time "1h"
    //time { 1.hour + 8.hour * (task.attempt-1) }
    //errorStrategy { task.exitStatus in 137..140 ? 'retry' : 'terminate' }
    //maxRetries 5

    publishDir "${params.tmp}/${params.config}/${params.subgraph}", overwrite: true

    input:
    path(summary_jsons)

    output:
    path("${params.subgraph}_summary.json")

    script:
    """
    #!/usr/bin/env bash
    set -Eeuo pipefail
    python3 ${params.home}/05_materialise/merge_summary_jsons.py ${summary_jsons} > ${params.subgraph}_summary.json
    """
}

process create_rocks {
    cache "lenient"
    memory "64 GB" 
    time "23h"
    cpus "8"
    errorStrategy 'retry'
    maxRetries 10

    publishDir "${params.tmp}/${params.config}/${params.subgraph}", overwrite: true

    input:
    val(materialised)

    output:
    path("${params.subgraph}_rocksdb")

    script:
    """
    #!/usr/bin/env bash
    set -Eeuo pipefail
    cat ${materialised.iterator().join(" ")} \
        | ${params.home}/target/release/grebi_make_rocks \
            --rocksdb-path /dev/shm/rocksdb && \
    mv /dev/shm/rocksdb ${params.subgraph}_rocksdb
    """
}

process prepare_neo {
    cache "lenient"
    memory "16 GB" 
    time "1h"

    publishDir "${params.tmp}/${params.config}/${params.subgraph}/neo4j_csv", overwrite: true

    input:
    path(prop_summary_json)
    path(nodes_jsonl)
    path(edges_jsonl)

    output:
    path("neo_nodes_${params.subgraph}_${task.index}.csv"), emit: nodes
    path("neo_edges_${params.subgraph}_${task.index}.csv"), emit: edges
    path("neo_edges_ids_${params.subgraph}_${task.index}.csv"), emit: id_edges

    script:
    """
    #!/usr/bin/env bash
    set -Eeuo pipefail
    ${params.home}/target/release/grebi_make_neo_csv \
      --in-summary-jsons ${prop_summary_json} \
      --in-nodes-jsonl ${nodes_jsonl} \
      --in-edges-jsonl ${edges_jsonl} \
      --out-nodes-csv-path neo_nodes_${params.subgraph}_${task.index}.csv \
      --out-edges-csv-path neo_edges_${params.subgraph}_${task.index}.csv \
      --out-id-edges-csv-path neo_edges_ids_${params.subgraph}_${task.index}.csv \
      --add-prefix ${params.subgraph}:
    """
}

process prepare_solr {
    cache "lenient"
    memory "4 GB" 
    time "1h"

    input:
    path(nodes_jsonl)
    path(edges_jsonl)

    output:
    path("solr_nodes_${params.subgraph}_${task.index}.jsonl"), emit: nodes
    path("solr_edges_${params.subgraph}_${task.index}.jsonl"), emit: edges

    script:
    """
    #!/usr/bin/env bash
    set -Eeuo pipefail
    ${params.home}/target/release/grebi_make_solr  \
      --in-nodes-jsonl ${nodes_jsonl} \
      --in-edges-jsonl ${edges_jsonl} \
      --out-nodes-jsonl-path solr_nodes_${params.subgraph}_${task.index}.jsonl \
      --out-edges-jsonl-path solr_edges_${params.subgraph}_${task.index}.jsonl
    """
}

process create_neo_ids_csv {
    cache "lenient"
    memory "8 GB" 
    time "8h"
    cpus "8"

    input:
    path(ids_txt)

    output:
    path("neo_nodes_ids_${params.subgraph}.csv")

    script:
    """
    #!/usr/bin/env bash
    set -Eeuo pipefail
    cat ${ids_txt} | ${params.home}/target/release/grebi_make_neo_ids_csv > neo_nodes_ids_${params.subgraph}.csv
    """
}

process create_neo {
    cache "lenient"
    memory "50 GB" 
    time "8h"
    cpus "16"

    publishDir "${params.tmp}/${params.config}/${params.subgraph}", overwrite: true

    input:
    path(neo_inputs)

    output:
    path("${params.subgraph}_neo4j")

    script:
    """
    #!/usr/bin/env bash
    set -Eeuo pipefail
    PYTHONUNBUFFERED=true python3 ${params.home}/07_create_db/neo4j/neo4j_import.slurm.py \
        --in-csv-path . \
        --out-db-path ${params.subgraph}_neo4j
    """
}

process create_solr_nodes_core {
    cache "lenient"
    memory "64 GB" 
    time "23h"
    cpus "16"
    
    publishDir "${params.tmp}/${params.config}/${params.subgraph}/solr_cores", overwrite: true, saveAs: { filename -> filename.replace("solr/data/", "") }

    input:
    path(solr_inputs)
    path(names_txt)

    output:
    path("solr/data/grebi_nodes_${params.subgraph}")

    script:
    """
    #!/usr/bin/env bash
    set -Eeuo pipefail
    python3 ${params.home}/06_prepare_db_import/make_solr_config.py \
        --subgraph-name ${params.subgraph} \
        --in-summary-json ${params.tmp}/${params.config}/${params.subgraph}/prop_summary.json \
        --in-template-config-dir ${params.home}/06_prepare_db_import/solr_config_template \
        --out-config-dir solr_config
    python3 ${params.home}/07_create_db/solr/solr_import.slurm.py \
        --solr-config solr_config --core grebi_nodes_${params.subgraph} --in-data . --out-path solr --port 8985 --mem ${task.memory.toGiga()-8}g
    """
}

process create_solr_edges_core {
    cache "lenient"
    memory "64 GB" 
    time "23h"
    cpus "16"

    publishDir "${params.tmp}/${params.config}/${params.subgraph}/solr_cores", overwrite: true, saveAs: { filename -> filename.replace("solr/data/", "") }

    input:
    path(solr_inputs)
    path(names_txt)

    output:
    path("solr/data/grebi_edges_${params.subgraph}")

    script:
    """
    #!/usr/bin/env bash
    set -Eeuo pipefail
    python3 ${params.home}/06_prepare_db_import/make_solr_config.py \
        --subgraph-name ${params.subgraph} \
        --in-summary-json ${params.tmp}/${params.config}/${params.subgraph}/prop_summary.json \
        --in-template-config-dir ${params.home}/06_prepare_db_import/solr_config_template \
        --out-config-dir solr_config
    python3 ${params.home}/07_create_db/solr/solr_import.slurm.py \
        --solr-config solr_config --core grebi_edges_${params.subgraph} --in-data . --out-path solr --port 8986 --mem ${task.memory.toGiga()-8}g
    """
}

process create_solr_autocomplete_core {
    cache "lenient"
    memory "64 GB" 
    time "4h"
    cpus "4"

    publishDir "${params.tmp}/${params.config}/${params.subgraph}/solr_cores", overwrite: true, saveAs: { filename -> filename.replace("solr/data/", "") }

    input:
    path(names_txt)

    output:
    path("solr/data/grebi_autocomplete_${params.subgraph}")

    script:
    """
    #!/usr/bin/env bash
    set -Eeuo pipefail
    python3 ${params.home}/06_prepare_db_import/make_solr_autocomplete_config.py \
        --subgraph-name ${params.subgraph} \
        --in-template-config-dir ${params.home}/06_prepare_db_import/solr_config_template \
        --out-config-dir solr_config
    python3 ${params.home}/07_create_db/solr/solr_import.slurm.py \
        --solr-config solr_config --core grebi_autocomplete_${params.subgraph} --in-data . --in-names-txt ${names_txt} --out-path solr --port 8987 --mem ${task.memory.toGiga()-8}g
    """
}

process package_neo {
    cache "lenient"
    memory "32 GB" 
    time "8h"
    cpus "16"

    publishDir "${params.tmp}/${params.config}/${params.subgraph}", overwrite: true

    input: 
    path("${params.subgraph}_neo4j")

    output:
    path("${params.subgraph}_neo4j.tgz")

    script:
    """
    tar -chf ${params.subgraph}_neo4j.tgz --use-compress-program="pigz --fast" ${params.subgraph}_neo4j
    """
}

process package_rocks {
    cache "lenient"
    memory "32 GB" 
    time "8h"
    cpus "16"

    publishDir "${params.tmp}/${params.config}/${params.subgraph}", overwrite: true

    input: 
    path("${params.subgraph}_rocksdb")

    output:
    path("${params.subgraph}_rocksdb.tgz")

    script:
    """
    tar -chf ${params.subgraph}_rocksdb.tgz --use-compress-program="pigz --fast" ${params.subgraph}_rocksdb
    """
}

process package_solr {
    cache "lenient"
    memory "32 GB" 
    time "8h"
    cpus "16"

    publishDir "${params.tmp}/${params.config}/${params.subgraph}", overwrite: true

    input: 
    path(solr_nodes_core)
    path(solr_edges_core)
    path(solr_autocomplete_core)

    output:
    path("${params.subgraph}_solr.tgz")

    script:
    """
    #!/usr/bin/env bash
    set -Eeuo pipefail
    cp -f ${params.home}/06_prepare_db_import/solr_config_template/*.xml .
    cp -f ${params.home}/06_prepare_db_import/solr_config_template/*.cfg .
    tar -chf ${params.subgraph}_solr.tgz --transform 's,^,solr/,' --use-compress-program="pigz --fast" \
	*.xml *.cfg ${solr_nodes_core} ${solr_edges_core} ${solr_autocomplete_core}
    """
}

process copy_neo_to_ftp {
    
    cache "lenient"
    memory "4 GB" 
    time "8h"
    queue "datamover"

    input: 
    path("neo4j.tgz")

    script:
    """
    #!/usr/bin/env bash
    set -Eeuo pipefail
    mkdir -p /nfs/ftp/public/databases/spot/kg/${params.config}/${params.timestamp.trim()}
    cp -f neo4j.tgz /nfs/ftp/public/databases/spot/kg/${params.config}/${params.timestamp.trim()}/${params.subgraph}_neo4j.tgz
    """
}

process copy_summary_to_ftp {
    
    cache "lenient"
    memory "4 GB" 
    time "8h"
    queue "datamover"

    input: 
    path(summary_json)

    script:
    """
    #!/usr/bin/env bash
    set -Eeuo pipefail
    mkdir -p /nfs/ftp/public/databases/spot/kg/${params.config}/${params.timestamp.trim()}
    cp -f ${summary_json} /nfs/ftp/public/databases/spot/kg/${params.config}/${params.timestamp.trim()}/
    """
}

process copy_solr_to_ftp {
    
    cache "lenient"
    memory "4 GB" 
    time "8h"
    queue "datamover"

    input: 
    path("solr.tgz")

    script:
    """
    #!/usr/bin/env bash
    set -Eeuo pipefail
    mkdir -p /nfs/ftp/public/databases/spot/kg/${params.config}/${params.timestamp.trim()}
    cp -f solr.tgz /nfs/ftp/public/databases/spot/kg/${params.config}/${params.timestamp.trim()}/${params.subgraph}_solr.tgz
    """
}

process copy_rocks_to_ftp {
    
    cache "lenient"
    memory "4 GB" 
    time "8h"
    queue "datamover"

    input: 
    path("rocksdb.tgz")

    script:
    """
    #!/usr/bin/env bash
    set -Eeuo pipefail
    mkdir -p /nfs/ftp/public/databases/spot/kg/${params.config}/${params.timestamp.trim()}
    cp -f rocksdb.tgz /nfs/ftp/public/databases/spot/kg/${params.config}/${params.timestamp.trim()}/${params.subgraph}_rocksdb.tgz
    """
}

process copy_summary_to_staging {
    
    cache "lenient"
    memory "4 GB" 
    time "8h"
    queue "datamover"

    input: 
    path(summary_json)

    script:
    """
    #!/usr/bin/env bash
    set -Eeuo pipefail
    mkdir -p /nfs/public/rw/ontoapps/grebi/staging/summaries
    cp -f ${summary_json} /nfs/public/rw/ontoapps/grebi/staging/summaries/
    """
}

process copy_solr_config_to_staging {
    cache "lenient"
    memory "4 GB" 
    time "8h"
    queue "datamover"

    script:
    """
    #!/usr/bin/env bash
    set -Eeuo pipefail
    cp -f ${params.home}/06_prepare_db_import/solr_config_template/*.xml .
    cp -f ${params.home}/06_prepare_db_import/solr_config_template/*.cfg .
    mkdir -p /nfs/public/rw/ontoapps/grebi/staging/solr
    cp -LR * /nfs/public/rw/ontoapps/grebi/staging/solr/
    """

}

process copy_solr_cores_to_staging {
    cache "lenient"
    memory "4 GB" 
    time "8h"
    queue "datamover"

    input: 
    path(solr_core)

    script:
    """
    #!/usr/bin/env bash
    set -Eeuo pipefail
    mkdir -p /nfs/public/rw/ontoapps/grebi/staging/solr
    cp -LR * /nfs/public/rw/ontoapps/grebi/staging/solr/
    """
}

process copy_rocksdb_to_staging {
    cache "lenient"
    memory "4 GB" 
    time "8h"
    queue "datamover"

    input: 
    path(rocksdb)

    script:
    """
    #!/usr/bin/env bash
    set -Eeuo pipefail
    mkdir -p /nfs/public/rw/ontoapps/grebi/staging/rocksdb
    cp -LR * /nfs/public/rw/ontoapps/grebi/staging/rocksdb/
    """
}


def parseJson(json) {
    return new JsonSlurper().parseText(json)
}

def getStdinCommand(ingest, filename) {
    if (ingest.stdin == false) {
        return ""
    }
    def f = filename
    if (filename.startsWith(".")) {
        f = new File(params.home, filename).toString()
    }
    if (f.endsWith(".gz")) {
        return "zcat ${f} |"
    } else if (f.endsWith(".xz")) {
        return "xzcat ${f} |"
    } else {
        return "cat ${f} |"
    }
}

def getIngestCommand(script) {
    return new File(params.home, script)
}

def buildIngestArgs(ingestArgs) {
    def res = ""
    ingestArgs.each { arg -> res += "${arg.name} ${arg.value} " }
    return res
}

def buildAddEquivGroupArgs(equivGroups) {
    def res = ""
    equivGroups.each { arg -> res += "--add-group ${arg.iterator().join(",")} " }
    return res
}

def buildMergeArgs(assigned) {
    def res = ""
    assigned.each { a ->
        res += "${a[0]}:${a[1]} "
    }
    return res
}

def basename(filename) {
    return new File(filename).name
}
