
nextflow.enable.dsl=2

import groovy.json.JsonSlurper
jsonSlurper = new JsonSlurper()

params.tmp = "$GREBI_TMP"
params.home = "$GREBI_HOME"
params.subgraph = "$GREBI_SUBGRAPH"

workflow {

    config = (new JsonSlurper().parse(new File(params.home, 'configs/subgraph_configs/' + params.subgraph + '.json')))

    files_listing = prepare() | splitText | map { row -> parseJson(row) }

    ingest(files_listing, Channel.value(config.equivalence_props))
    groups_txt = build_equiv_groups(ingest.out.equivalences.collect(), Channel.value(config.additional_equivalence_groups))
    assigned = assign_ids(ingest.out.nodes, groups_txt).collect(flat: false)

    merged = merge_ingests(
        assigned,
        Channel.value(config.exclude_props),
        Channel.value(config.bytes_per_merged_file))

    indexed = index(merged.collect())
    materialise(merged.flatten(), indexed, Channel.value(config.exclude_edges + config.equivalence_props))
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
    val(equivalence_props)

    output:
    tuple val(file_listing.datasource.name), path("nodes_${task.index}.jsonl.gz"), emit: nodes
    path("equivalences_${task.index}.tsv"), emit: equivalences

    script:
    """
    #!/usr/bin/env bash
    set -Eeuo pipefail
    ${getDecompressionCommand(file_listing.filename)} \
        | ${getIngestCommand(file_listing.ingest.ingest_script)} \
            --datasource-name ${file_listing.datasource.name} \
            --filename "${basename(file_listing.filename)}" \
            ${buildIngestArgs(file_listing.ingest.ingest_args)} \
        | ${params.home}/target/release/grebi_normalise_prefixes ${params.home}/prefix_maps/prefix_map_normalise.json \
        | tee >(${params.home}/target/release/grebi_extract_equivalences \
                --equivalence-properties ${equivalence_props.iterator().join(",")} \
                    > equivalences_${task.index}.tsv) \
        | pigz --fast > nodes_${task.index}.jsonl.gz
    """
}

process build_equiv_groups {
    cache "lenient"
    memory '64 GB'
    time '23h'

    input:
    path(equivalences_tsv)
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
    cache "lenient"
    memory { 32.GB + 32.GB * (task.attempt-1) }
    time { 1.hour + 8.hour * (task.attempt-1) }
    errorStrategy { task.exitStatus in 137..140 ? 'retry' : 'terminate' }
    maxRetries 5

    input:
    tuple(val(datasource_name), path(nodes_jsonl))
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
        ${buildMergeArgs(assigned)} \
        | split -a 6 -d -C ${bytes_per_merged_file} - merged.jsonl.
    """
}

process index {
    cache "lenient"
    memory "64 GB" 
    time "8h"

    publishDir "${params.tmp}/${params.subgraph}", overwrite: true

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
        --subgraph-name {params.subgraph} \
        --out-metadata-jsonl-path metadata.jsonl \
        --out-summary-json-path summary.json \
        --out-names-txt names.txt
    """
}

process materialise {
    cache "lenient"
    //memory { 80.GB + 30.GB * (task.attempt-1) }
    //memory "32 GB"
    memory "96 GB"
    time "8h"
    //time { 1.hour + 8.hour * (task.attempt-1) }
    //errorStrategy { task.exitStatus in 137..140 ? 'retry' : 'terminate' }
    //maxRetries 5

    publishDir "${params.tmp}/${params.subgraph}/nodes", overwrite: true, pattern: 'nodes_*'
    publishDir "${params.tmp}/${params.subgraph}/edges", overwrite: true, pattern: 'edges_*'

    input:
    path(merged_filename)
    tuple(path(metadata_jsonl), path(summary_json), path(names_txt))
    val(exclude)

    output:
    path("nodes_${task.index}.jsonl")
    path("edges_${task.index}.jsonl")

    script:
    """
    #!/usr/bin/env bash
    set -Eeuo pipefail
    cat ${merged_filename} \
        | ${params.home}/target/release/grebi_materialise \
          --in-metadata-jsonl ${metadata_jsonl} \
          --out-edges-jsonl edges_${task.index}.jsonl \
          --exclude ${exclude.iterator().join(",")} \
        > nodes_${task.index}.jsonl
    """
}

def parseJson(json) {
    return new JsonSlurper().parseText(json)
}

def getDecompressionCommand(filename) {
    def f = ""
    if (filename.startsWith(".")) {
        f = new File(params.home, filename).toString()
    } else {
        f = filename
    }
    if (f.endsWith(".gz")) {
        return "zcat ${f}"
    } else if (f.endsWith(".xz")) {
        return "xzcat ${f}"
    } else {
        return "cat ${f}"
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
