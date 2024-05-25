
nextflow.enable.dsl=2

import groovy.json.JsonSlurper
jsonSlurper = new JsonSlurper()

params.home = "$GREBI_HOME"
params.config = "$GREBI_CONFIG"

workflow {

    config = (new JsonSlurper().parse(
        new File(params.home + "/configs/pipeline_configs/" + params.config + ".json")))

    files_listing = prepare() | splitText | map { row -> parseJson(row) }
    ingested = ingest(files_listing, Channel.value(config.equivalence_props))
    groups_txt = build_equiv_groups(ingested, Channel.value(config.additional_equivalence_groups))
    assigned = assign_ids(ingested, groups_txt).collect(flat: false)

    merged = merge_ingests(
        assigned,
        Channel.value(config.exclude_props),
        Channel.value(config.bytes_per_merged_file))

    indexed = index(merged.collect())
    materialised = materialise(merged.flatten(), indexed, Channel.value(config.exclude_edges + config.equivalence_props))

    rocks_db = create_rocks(materialised.collect())

    neo_input_dir = prepare_neo(indexed, materialised)
    neo_db = create_neo(prepare_neo.out.nodes.collect() + prepare_neo.out.edges.collect())

    solr_inputs = prepare_solr(materialised)
    solr_nodes_core = create_solr_nodes_core(prepare_solr.out.nodes.collect(), indexed)
    solr_edges_core = create_solr_edges_core(prepare_solr.out.edges.collect(), indexed)
    solr_autocomplete_core = create_solr_autocomplete_core(indexed)

    package_solr(solr_nodes_core, solr_edges_core, solr_autocomplete_core)
    package_neo(neo_db)
    package_rocks(rocks_db)
}

process prepare {
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
    memory { 4.GB + 32.GB * (task.attempt-1) }
    time { 1.hour + 8.hour * (task.attempt-1) }
    errorStrategy { task.exitStatus in 137..140 ? 'retry' : 'terminate' }
    maxRetries 5
    
    input:
    val(file_listing)
    val(equivalence_props)

    output:
    tuple(val(file_listing.datasource.name), path("nodes.jsonl.gz"), path("equivalences.tsv"))

    script:
    """
    #!/usr/bin/env bash
    set -Eeuo pipefail
    ${getDecompressionCommand(file_listing.filename)} \
        | ${new File(params.home, file_listing.ingest.ingest_script)} \
            --datasource-name ${file_listing.datasource.name} \
            --filename "${basename(file_listing.filename)}" \
            ${buildIngestArgs(file_listing.ingest.ingest_args)} \
        | ${params.home}/target/release/grebi_normalise_prefixes ${params.home}/prefix_maps/prefix_map_normalise.json \
        | tee >(${params.home}/target/release/grebi_extract_equivalences \
                --equivalence-properties ${equivalence_props.iterator().join(",")} \
                    > equivalences.tsv) \
        | pigz --fast > nodes.jsonl.gz
    """
}

process build_equiv_groups {
    memory '64 GB'
    time '23h'

    input:
    tuple(val(datasource_name), path(nodes_jsonl), path(equivalences_tsv))
    val(additional_equivalence_groups)

    output:
    path "groups.txt"

    script:
    """
    #!/usr/bin/env bash
    set -Eeuo pipefail
    cat ${equivalences_tsv} \
        | ${params.home}/target/release/grebi_equivalences2groups \
            ${buildAddEquivGroupArgs(additional_equivalence_groups)} \
        > groups.txt
    """
}

process assign_ids {
    memory { 32.GB + 32.GB * (task.attempt-1) }
    time { 1.hour + 8.hour * (task.attempt-1) }
    errorStrategy { task.exitStatus in 137..140 ? 'retry' : 'terminate' }
    maxRetries 5

    input:
    tuple(val(datasource_name), path(nodes_jsonl), path(equivalences_tsv))
    path groups_txt

    output:
    tuple(val(datasource_name), path("nodes_with_ids.sorted.jsonl.gz"))

    script:
    """
    #!/usr/bin/env bash
    set -Eeuo pipefail
    zcat ${nodes_jsonl} \
        | ${params.home}/target/release/grebi_assign_ids \
            --groups-txt ${groups_txt} \
        > nodes_with_ids.jsonl
    LC_ALL=C sort -o nodes_with_ids.sorted.jsonl nodes_with_ids.jsonl
    rm -f nodes_with_ids.jsonl
    pigz --fast nodes_with_ids.sorted.jsonl
    """
}

process merge_ingests {
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
        ${buildMergeArgs(assigned)} \
        | split -a 6 -d -C ${bytes_per_merged_file} - merged.jsonl.
    """
}

process index {
    memory "64 GB" 
    time "8h"

    input:
    val(merged_filenames)

    output:
    tuple(path("metadata.jsonl"), path("summary.json"), path("names.txt"))

    script:
    """
    #!/usr/bin/env bash
    set -Eeuo pipefail
    cat ${merged_filenames.iterator().join(" ")} \
        | ${params.home}/target/release/grebi_index \
        --out-metadata-jsonl-path metadata.jsonl \
        --out-summary-json-path summary.json \
        --out-names-txt names.txt
    """
}

process materialise {
    memory "100 GB" 
    time "1h"

    input:
    path(merged_filename)
    tuple(path(metadata_jsonl), path(summary_json), path(names_txt))
    val(exclude)

    output:
    tuple(path("nodes.jsonl"), path("edges.jsonl"))

    script:
    """
    #!/usr/bin/env bash
    set -Eeuo pipefail
    cat ${merged_filename} \
        | ${params.home}/target/release/grebi_materialise \
          --in-metadata-jsonl ${metadata_jsonl} \
          --in-summary-json ${summary_json} \
          --out-edges-jsonl edges.jsonl \
          --exclude ${exclude.iterator().join(",")} \
        > nodes.jsonl
    """
}

process create_rocks {

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
            --rocksdb-path rocksdb
    """
}

process prepare_neo {
    memory "4 GB" 
    time "1h"

    input:
    tuple(path(metadata_jsonl), path(summary_json), path(names_txt))
    tuple(path(nodes_jsonl), path(edges_jsonl))

    output:
    path("neo_nodes_${task.index}.csv"), emit: nodes
    path("neo_edges_${task.index}.csv"), emit: edges

    script:
    """
    #!/usr/bin/env bash
    set -Eeuo pipefail
    ${params.home}/target/release/grebi_make_csv \
      --in-summary-json ${summary_json} \
      --in-nodes-jsonl ${nodes_jsonl} \
      --in-edges-jsonl ${edges_jsonl} \
      --out-nodes-csv-path neo_nodes_${task.index}.csv \
      --out-edges-csv-path neo_edges_${task.index}.csv
    """
}

process prepare_solr {
    memory "4 GB" 
    time "1h"

    input:
    tuple(path(nodes_jsonl), path(edges_jsonl))

    output:
    path("solr_nodes_${task.index}.jsonl"), emit: nodes
    path("solr_edges_${task.index}.jsonl"), emit: edges

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
    memory "100 GB" 
    time "8h"

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
    memory "64 GB" 
    time "23h"

    input:
    path(solr_inputs)
    tuple(path(metadata_jsonl), path(summary_json), path(names_txt))

    output:
    path("grebi_nodes")

    script:
    """
    #!/usr/bin/env bash
    set -Eeuo pipefail
    PYTHONUNBUFFERED=true python3 ${params.home}/07_create_db/solr/solr_import.slurm.py \
        --core grebi_nodes --in-data . --in-names-txt ${names_txt} --out-path grebi_nodes --port 8985
    """
}

process create_solr_edges_core {
    memory "64 GB" 
    time "23h"

    input:
    path(solr_inputs)
    tuple(path(metadata_jsonl), path(summary_json), path(names_txt))

    output:
    path("grebi_edges")

    script:
    """
    #!/usr/bin/env bash
    set -Eeuo pipefail
    PYTHONUNBUFFERED=true python3 ${params.home}/07_create_db/solr/solr_import.slurm.py \
        --core grebi_edges --in-data . --in-names-txt ${names_txt} --out-path grebi_edges --port 8986
    """
}

process create_solr_autocomplete_core {
    memory "64 GB" 
    time "4h"

    input:
    tuple(path(metadata_jsonl), path(summary_json), path(names_txt))

    output:
    path("grebi_autocomplete")

    script:
    """
    #!/usr/bin/env bash
    set -Eeuo pipefail
    PYTHONUNBUFFERED=true python3 ${params.home}/07_create_db/solr/solr_import.slurm.py \
        --core grebi_autocomplete --in-data . --in-names-txt ${names_txt} --out-path grebi_autocomplete --port 8987
    """
}

process package_neo {
    memory "4 GB" 
    time "8h"

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
    memory "4 GB" 
    time "8h"

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
    memory "4 GB" 
    time "8h"

    publishDir "${params.home}/release/${params.config}", overwrite: true

    input: 
    path(solr_nodes_core)
    path(solr_edges_core)
    path(solr_autocomplete_core)

    output:
    path("solr.tgz")

    script:
    """
    #!/usr/bin/env bash
    set -Eeuo pipefail
    cp -f ${params.home}/07_create_db/solr/solr_config/*.xml .
    cp -f ${params.home}/07_create_db/solr/solr_config/*.cfg .
    tar -chf solr.tgz --transform 's,^,solr/,' --use-compress-program="pigz --fast" \
	*.xml *.cfg ${solr_nodes_core} ${solr_edges_core} ${solr_autocomplete_core}
    """
}


def parseJson(json) {
    return new JsonSlurper().parseText(json)
}

def getDecompressionCommand(filename) {
    if (filename.endsWith(".gz")) {
        return "zcat ${filename}"
    } else if (filename.endsWith(".xz")) {
        return "xzcat ${filename}"
    } else {
        return "cat ${filename}"
    }
}

def buildIngestArgs(ingestArgs) {
    res = ""
    ingestArgs.each { arg -> res += "${arg.name} ${arg.value} " }
    return res
}

def buildAddEquivGroupArgs(equivGroups) {
    res = ""
    equivGroups.each { arg -> res += "--add-group ${arg.iterator().join(",")} " }
    return res
}

def buildMergeArgs(assigned) {
    res = ""
    assigned.each { a ->
        res += "${a[0]}:${a[1]} "
    }
    return res
}

def basename(filename) {
    return new File(filename).name
}
