process ingest {
    cache "lenient"
    memory { (file_listing.ingest.memory ? MemoryUnit.of(file_listing.ingest.memory) : 4.GB) + 128.GB * (task.attempt-1) }
    time { 1.hour + 8.hour * (task.attempt-1) }
    errorStrategy { task.exitStatus in 137..140 ? 'retry' : 'terminate' }
    maxRetries 5
    
    input:
    // input_file is the same file as file_listing.filename, declared as a path
    // so that resume tracks its content; the script reads it by absolute path.
    tuple val(subgraph), val(file_listing), val(identifier_props), val(bytes_per_merged_file), path(input_file, stageAs: 'input/*')

    output:
    tuple val(subgraph), val(file_listing.datasource.id), path("nodes_${task.index}.jsonl.*"), emit: nodes
    tuple val(subgraph), path("identifiers_${task.index}.tsv"), emit: identifiers

    script:
    """
    #!/usr/bin/env bash
    set -Eeuo pipefail
    export GREBI_DATASOURCE_ID=${file_listing.datasource.id}
    export GREBI_INGEST_DATASOURCE_NAME=${file_listing.datasource.id}
    export GREBI_INGEST_FILENAME=${file_listing.filename}
    export GREBI_DATALOAD_HOME=/opt/grebi_dataload
    echo "Ingesting: \$GREBI_INGEST_FILENAME"
    ${file_listing.ingest.command} \
        | grebi_normalise_prefixes /opt/grebi_dataload/prefix_maps/prefix_map_normalise.json \
        | tee >(grebi_extract_identifiers \
                --identifier-properties ${identifier_props.iterator().join(",")} \
                    > identifiers_${task.index}.tsv) \
        | split -a 6 -d -C ${bytes_per_merged_file} - nodes_${task.index}.jsonl.
    """
}
