def getStdinCommand(ingest, filename) {
    if (ingest.stdin == false) {
        return ""
    }
    def f = new File(filename.toString()).getName()
    if (f.endsWith(".gz")) {
        return "zcat ${f} |"
    } else if (f.endsWith(".xz")) {
        return "xzcat ${f} |"
    } else {
        return "cat ${f} |"
    }
}

process ingest {
    cache "lenient"
    memory { 4.GB + 128.GB * (task.attempt-1) }
    time { 1.hour + 8.hour * (task.attempt-1) }
    errorStrategy { task.exitStatus in 137..140 ? 'retry' : 'terminate' }
    maxRetries 5
    
    input:
    val(file_listing)
    path(filename)
    val(identifier_props)
    val(bytes_per_merged_file)

    output:
    tuple val(file_listing.datasource.name), path("nodes_${task.index}.jsonl.*"), emit: nodes
    path("identifiers_${task.index}.tsv"), emit: identifiers

    script:
    """
    #!/usr/bin/env bash
    set -Eeuo pipefail
    export GREBI_INGEST_DATASOURCE_NAME=${file_listing.datasource.name}
    export GREBI_INGEST_FILENAME=${filename}
    export GREBI_DATALOAD_HOME=/opt/grebi_dataload
    echo "Files in ingest working dir: \$(ls)"
    ${getStdinCommand(file_listing.ingest, filename)} \
        ${file_listing.ingest.command} \
        | grebi_normalise_prefixes /opt/grebi_dataload/prefix_maps/prefix_map_normalise.json \
        | tee >(grebi_extract_identifiers \
                --identifier-properties ${identifier_props.iterator().join(",")} \
                    > identifiers_${task.index}.tsv) \
        | split -a 6 -d -C ${bytes_per_merged_file} - nodes_${task.index}.jsonl.
    """
}
