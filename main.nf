
nextflow.enable.dsl=2

import groovy.json.JsonSlurper
jsonSlurper = new JsonSlurper()

params.home = "$GREBI_HOME"
params.config = "$GREBI_CONFIG"

workflow {

    config = (new JsonSlurper().parse(
        new File(params.home + "/configs/pipeline_configs/" + params.config + ".json")))

    files_listing = prepare() | splitText | map { row -> parseJson(row) }

    ingest(Channel.value(config), files_listing)
}

process prepare {
    output:
    file "datasource_files.jsonl"

    script: 
    """
    python3 ${params.home}/scripts/dataload_00_prepare.py
    """
}

process ingest {
    input:
    val(config)
    val(file_listing)

    output:
    file "nodes.jsonl"
    file "equivalences.tsv"

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
                --equivalence-properties ${config.equivalence_props.iterator().join(",")} \
                    > equivalences.tsv) \
        > nodes.jsonl
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

def basename(filename) {
    return new File(filename).name
}
