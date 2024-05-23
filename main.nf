
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
            .collect(flat: false)

    indexed = index(merged)
    materialised = materialise(merged, indexed, Channel.value(config.exclude_edges + config.equivalence_props))

    neo = prepare_neo(indexed, materialised)
    solr = prepare_solr(materialised)
}

process prepare {
    output:
    path "datasource_files.jsonl"

    script: 
    """
    python3 ${params.home}/scripts/dataload_00_prepare.py
    """
}

process ingest {
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
            ${buildIngestArgs(file_listing.ingest_args)} \
        | ${params.home}/target/release/grebi_normalise_prefixes ${params.home}/prefix_maps/prefix_map_normalise.json \
        | tee >(${params.home}/target/release/grebi_extract_equivalences \
                --equivalence-properties ${equivalence_props.iterator().join(",")} \
                    > equivalences.tsv) \
        | pigz --fast > nodes.jsonl.gz
    """
}

process build_equiv_groups {

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

    input:
    val(assigned)
    val(exclude_props)
    val(bytes_per_merged_file)

    output:
    file('merged.jsonl.*')

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

process prepare_neo {

    input:
    tuple(path(metadata_jsonl), path(summary_json), path(names_txt))
    tuple(path(nodes_jsonl), path(edges_jsonl))

    output:
    tuple(path("neo_nodes.csv"), path("neo_edges.csv"))

    script:
    """
    ${params.home}/target/release/grebi_make_csv \
      --in-summary-json ${summary_json} \
      --in-nodes-jsonl ${nodes_jsonl} \
      --in-edges-jsonl ${edges_jsonl} \
      --out-nodes-csv-path neo_nodes.csv \
      --out-edges-csv-path neo_edges.csv
    """
}

process prepare_solr {

    input:
    tuple(path(nodes_jsonl), path(edges_jsonl))

    output:
    tuple(path("solr_nodes.jsonl"), path("solr_edges.jsonl"))

    script:
    """
    ${params.home}/target/release/grebi_make_solr  \
      --in-nodes-jsonl ${nodes_jsonl} \
      --in-edges-jsonl ${edges_jsonl} \
      --out-nodes-jsonl-path solr_nodes.jsonl \
      --out-edges-jsonl-path solr_edges.jsonl
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
    ingestArgs.each { arg -> res += "--${arg.name} ${arg.value} " }
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
