
nextflow.enable.dsl=2

import groovy.json.JsonSlurper
jsonSlurper = new JsonSlurper()

import groovy.yaml.YamlSlurper
yamlSlurper = new YamlSlurper()

params.out = "$GREBI_OUT_DIR"
params.subgraph = "$GREBI_SUBGRAPH"
params.query_yamls_path = "$GREBI_QUERY_YAMLS_PATH"
params.solr_mem = "140g"
params.neo_tmp_path = "/dev/shm"
params.dataload_home = "$GREBI_DATALOAD_HOME"
params.dataload_home_inside_container = "/opt/grebi_dataload"

workflow {

    config = (new JsonSlurper().parse(new File(params.dataload_home, 'configs/subgraph_configs/' + params.subgraph + '.json')))

    datasources = config.datasource_configs.collect { ds -> new YamlSlurper().parse(new File(params.dataload_home, ds)) }

    datasource_files = Channel.from(datasources.collect {
        ds -> ds.ingests.collect {
            ingest -> ingest.globs.collect {
                glob -> files(glob).collect {
                    file -> [
                        datasource: ds,
                        ingest: ingest,
                        filename: file.toString()
                    ]
                }
            }
        }
     }) | flatten

    datasource_files | view
     
    ingest(datasource_files, datasource_files | map { listing -> listing.filename }, Channel.value(config.identifier_props))

    groups_txt = build_equiv_groups(ingest.out.identifiers.collect(), Channel.value(config.additional_equivalence_groups))
    assigned = assign_ids(ingest.out.nodes, groups_txt, Channel.value(config.identifier_props), Channel.value(config.type_superclasses)).collect(flat: false)

    merged = merge_ingests(
        assigned,
        Channel.value(config.exclude_props),
        Channel.value(config.bytes_per_merged_file))

    indexed = index(merged.collect())

    link(merged.flatten(), indexed.entity_metadata_jsonl, indexed.graph_metadata_json, Channel.value(config.exclude_edges + config.identifier_props), Channel.value(config.exclude_self_referential_edges + config.identifier_props), groups_txt)
    merge_graph_metadata_jsons(indexed.graph_metadata_json.collect() + link.out.linked_summary.collect())

    compressed_blobs = create_compressed_blobs(link.out.nodes.mix(link.out.edges))
    sqlite = create_sqlite(compressed_blobs.collect())

    neo_input_dir = prepare_neo(indexed.graph_metadata_json, link.out.nodes, link.out.edges)

    ids_csv = create_neo_ids_csv(indexed.ids_txt)
    neo_db = create_neo(
        prepare_neo.out.nodes.collect() +
        prepare_neo.out.edges.collect() +
        prepare_neo.out.id_edges.collect() +
        ids_csv.collect()
    )

    run_materialised_queries(neo_db, params.query_yamls_path)

    csv_results = results_to_csv(run_materialised_queries.out.results.flatten())
    linked_results = link_results(run_materialised_queries.out.results.flatten(), indexed.entity_metadata_jsonl, groups_txt)

    add_query_metadatas_to_graph_metadata(run_materialised_queries.out.metadata.flatten().collect(), merge_graph_metadata_jsons.out)

    solr_inputs = prepare_solr(link.out.nodes, link.out.edges)
    solr_nodes_core = create_solr_nodes_core(prepare_solr.out.nodes.collect(), indexed.names_txt, merge_graph_metadata_jsons.out)
    solr_edges_core = create_solr_edges_core(prepare_solr.out.edges.collect(), indexed.names_txt, merge_graph_metadata_jsons.out)
    solr_autocomplete_core = create_solr_autocomplete_core(indexed.names_txt)
    solr_results_cores = create_solr_results_cores(linked_results)

    solr_tgz = package_solr(solr_nodes_core.concat(solr_edges_core).concat(solr_autocomplete_core).concat(solr_results_cores).collect())
    neo_tgz = package_neo(neo_db)
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

    output:
    tuple val(file_listing.datasource.name), path("nodes_${task.index}.jsonl.gz"), emit: nodes
    path("identifiers_${task.index}.tsv"), emit: identifiers

    script:
    """
    #!/usr/bin/env bash
    set -Eeuo pipefail
    export GREBI_INGEST_DATASOURCE_NAME=${file_listing.datasource.name}
    export GREBI_INGEST_FILENAME=${filename}
    echo "Files in ingest working dir: \$(ls)"
    ${getStdinCommand(file_listing.ingest, filename)} \
        ${file_listing.ingest.command} \
        | grebi_normalise_prefixes ${params.dataload_home_inside_container}/prefix_maps/prefix_map_normalise.json \
        | tee >(grebi_extract_identifiers \
                --identifier-properties ${identifier_props.iterator().join(",")} \
                    > identifiers_${task.index}.tsv) \
        | pigz --fast > nodes_${task.index}.jsonl.gz
    """
}

process build_equiv_groups {
    cache "lenient"
    memory '4 GB'
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
        | grebi_identifiers2groups \
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
        | grebi_assign_ids \
            --identifier-properties ${identifier_props.iterator().join(",")} \
            --groups-txt ${groups_txt} \
        | grebi_superclasses2types \
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
    memory "4 GB" 
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
    grebi_merge \
        --exclude-props ${exclude_props.iterator().join(",")} \
        --annotate-subgraph-name ${params.subgraph} \
        ${buildMergeArgs(assigned)} \
        | split -a 6 -d -C ${bytes_per_merged_file} - merged.jsonl.
    """
}

process index {
    cache "lenient"
    memory "4 GB" 
    time "8h"

    input:
    val(merged_filenames)

    output:
    path("entity_metadata.jsonl"), emit: entity_metadata_jsonl
    path("graph_metadata.json"), emit: graph_metadata_json
    path("names.txt"), emit: names_txt
    path("ids_${params.subgraph}.txt"), emit: ids_txt

    script:
    """
    #!/usr/bin/env bash
    set -Eeuo pipefail
    cat ${merged_filenames.iterator().join(" ")} \
        | grebi_index \
        --subgraph-name ${params.subgraph} \
        --out-entity-metadata-jsonl-path entity_metadata.jsonl \
        --out-graph-metadata-json-path graph_metadata.json \
        --out-names-txt names.txt \
        --out-ids-txt ids_${params.subgraph}.txt
    """
}

process link {
    cache "lenient"
    memory "4 GB"
    time "8h"
    //time { 1.hour + 8.hour * (task.attempt-1) }
    //errorStrategy { task.exitStatus in 137..140 ? 'retry' : 'terminate' }
    //maxRetries 5

    input:
    path(merged_filename)
    path(entity_metadata_jsonl)
    path(index_graph_metadata_json)
    val(exclude)
    val(exclude_self_referential)
    path(groups_txt)

    output:
    path("linked_nodes_${task.index}.jsonl"), emit: nodes
    path("linked_edges_${task.index}.jsonl"), emit: edges
    path("linked_graph_metadata_${task.index}.json"), emit: linked_summary

    script:
    """
    #!/usr/bin/env bash
    set -Eeuo pipefail
    cat ${merged_filename} \
        | grebi_link \
          --in-metadata-jsonl ${entity_metadata_jsonl} \
          --in-graph-metadata-json ${index_graph_metadata_json} \
          --groups-txt ${groups_txt} \
          --out-edges-jsonl linked_edges_${task.index}.jsonl \
          --out-graph-metadata-json linked_graph_metadata_${task.index}.json \
          --exclude ${exclude.iterator().join(",")} \
          --exclude-self-referential ${exclude_self_referential.iterator().join(",")} \
        > linked_nodes_${task.index}.jsonl
    """
}

process merge_graph_metadata_jsons {
    cache "lenient"
    memory "4 GB"
    time "1h"

    input:
    path(graph_metadata_jsons)

    output:
    path("${params.subgraph}_metadata_merged.json")

    script:
    """
    #!/usr/bin/env bash
    set -Eeuo pipefail
    python3 ${params.dataload_home_inside_container}/05_link/merge_graph_metadata_jsons.py ${graph_metadata_jsons} > ${params.subgraph}_metadata_merged.json
    """
}

process create_compressed_blobs {
    cache "lenient"
    memory "16 GB"
    time "1h"

    input:
    path(mat_jsonl)

    output:
    path("${params.subgraph}_${task.index}_compressed.blob"), emit: compressed_blob

    script:
    """
    #!/usr/bin/env bash
    set -Eeuo pipefail
    cat ${mat_jsonl} | grebi_make_compressed_blob > ${params.subgraph}_${task.index}_compressed.blob
    """
}

process create_sqlite {
    cache "lenient"
    memory "128 GB" 
    time "23h"
    cpus "8"
    errorStrategy 'retry'
    maxRetries 10

    publishDir "${params.out}", overwrite: true

    input:
    val(compressed_blobs)

    output:
    path("${params.subgraph}.sqlite3")

    script:
    """
    #!/usr/bin/env bash
    set -Eeuo pipefail
    cat ${compressed_blobs.iterator().join(" ")} \
        | grebi_make_sqlite \
            --db-path ${params.subgraph}.sqlite3 \
            --batch-size 450 \
            --page-size 16384 \
            --cache-size 1000000
    """
}

process prepare_neo {
    cache "lenient"
    memory "4 GB" 
    time "1h"

    input:
    path(graph_metadata_json)
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
    grebi_make_neo_csv \
      --in-graph-metadata-jsons ${graph_metadata_json} \
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
    grebi_make_solr  \
      --in-nodes-jsonl ${nodes_jsonl} \
      --in-edges-jsonl ${edges_jsonl} \
      --out-nodes-jsonl-path solr_nodes_${params.subgraph}_${task.index}.jsonl \
      --out-edges-jsonl-path solr_edges_${params.subgraph}_${task.index}.jsonl
    """
}

process create_neo_ids_csv {
    cache "lenient"
    memory "4 GB" 
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
    cat ${ids_txt} | grebi_make_neo_ids_csv > neo_nodes_ids_${params.subgraph}.csv
    """
}

process create_neo {
    cache "lenient"
    memory "4 GB" 
    time "8h"
    cpus "8"
    scratch "ram-disk"

    input:
    path(neo_inputs)

    output:
    path("${params.subgraph}_neo4j")

    script:
    """
    #!/usr/bin/env bash
    set -Eeuo pipefail
    export NEO4J_HOME=\$(pwd)/${params.subgraph}_neo4j
    PYTHONUNBUFFERED=true ${params.dataload_home_inside_container}/06_create_neo_db/neo4j_import.dockersh \
        --out-db-path ${params.subgraph}_neo4j
    """
}

process run_materialised_queries {
    cache "lenient"
    memory "8 GB" 
    time "48h"
    cpus "8"
    scratch "ram-disk"

    publishDir "${params.out}", overwrite: true

    input:
    path(neo_db)
    path(query_yamls_path)

    output:
    path("query_results/queries.json"), emit: metadata
    path("query_results/*.results.jsonl"), emit: results
    path("query_results/*.json"), emit: metadatas

    script:
    """
    #!/usr/bin/env bash
    set -Eeuo pipefail
    export NEO4J_HOME=\$(pwd)/${neo_db}
    export GREBI_SUBGRAPH=${params.subgraph}
    mkdir query_results
    PYTHONUNBUFFERED=true python3 ${params.dataload_home_inside_container}/07_run_queries/run_queries.dockerpy ${query_yamls_path}
    """
}

process results_to_csv {
    cache "lenient"
    memory "8 GB" 
    time "8h"
    cpus "8"

    publishDir "${params.out}", overwrite: true

    input:
    path(results_jsonl)

    output:
    path("query_results/${results_jsonl.simpleName}.csv.gz")

    script:
    """
    #!/usr/bin/env bash
    set -Eeuo pipefail
    mkdir query_results
    cat ${results_jsonl} | \
    python3 ${params.dataload_home_inside_container}/07_run_queries/jsonl_to_csv.py \
    | pigz --best > query_results/${results_jsonl.simpleName}.results.csv.gz
    """
}

process link_results {
    cache "lenient"
    memory "8 GB" 
    time "8h"
    cpus "8"

    input:
    path(results_jsonl)
    path(entity_metadata_jsonl)
    path(groups_txt)

    output:
    path("${results_jsonl.simpleName}.linked_results.jsonl")

    script:
    """
    #!/usr/bin/env bash
    set -Eeuo pipefail
    cat ${results_jsonl} | \
    ${params.dataload_home_inside_container}/target/release/grebi_link_results \
          --in-metadata-jsonl ${entity_metadata_jsonl} \
          --groups-txt ${groups_txt} \
          > ${results_jsonl.simpleName}.linked_results.jsonl
    """
}

process add_query_metadatas_to_graph_metadata {

    cache "lenient"
    memory "8 GB" 
    time "8h"
    cpus "8"

    publishDir "${params.out}", overwrite: true

    input:
    path(metadata_jsons)
    path(graph_metadata_json)

    output:
    path("${params.subgraph}_metadata.json")

    script:
    """
    #!/usr/bin/env bash
    set -Eeuo pipefail
    python3 ${params.dataload_home_inside_container}/07_run_queries/add_query_metadatas_to_graph_metadata.py \
        ${graph_metadata_json} \
        ${metadata_jsons} \
        > ${params.subgraph}_metadata.json
    """
}

process csvs_to_sqlite {
    cache "lenient"
    memory "64 GB" 
    time "12h"
    cpus "8"

    publishDir "${params.out}", overwrite: true

    input:
    path(csvs)

    output:
    path("materialised_queries.sqlite3.gz")

    script:
    """
    #!/usr/bin/env bash
    set -Eeuo pipefail
    cp -r ${neo_db}/* ${params.neo_tmp_path}
    PYTHONUNBUFFERED=true python3 ${params.dataload_home_inside_container}/07_run_queries/csvs_to_sqlite.py --out-sqlite-path materialised_queries.sqlite3
    pigz --best materialised_queries.sqlite3
    """
}


process create_solr_nodes_core {
    cache "lenient"
    memory "4 GB" 
    time "23h"
    cpus "8"
    scratch "ram-disk"

    input:
    path(solr_inputs)
    path(names_txt)
    path(graph_metadata_json)

    output:
    path("solr/data/grebi_nodes_${params.subgraph}")

    script:
    """
    #!/usr/bin/env bash
    set -Eeuo pipefail
    mkdir -p solr/data
    python3 ${params.dataload_home_inside_container}/08_create_other_dbs/solr/make_solr_config.py \
        --subgraph-name ${params.subgraph} \
        --in-graph-metadata-json ${graph_metadata_json} \
        --in-template-config-dir ${params.dataload_home_inside_container}/08_create_other_dbs/solr/solr_config_template \
        --out-config-dir ./solr/data
    python3 ${params.dataload_home_inside_container}/08_create_other_dbs/solr/solr_import.dockerpy \
        grebi_nodes_${params.subgraph} 8985 ${params.solr_mem}
    """
}

process create_solr_edges_core {
    cache "lenient"
    memory "1500 GB" 
    time "23h"
    cpus "8"
    scratch "ram-disk"

    input:
    path(solr_inputs)
    path(names_txt)
    path(graph_metadata_json)

    output:
    path("solr/data/grebi_edges_${params.subgraph}")

    script:
    """
    #!/usr/bin/env bash
    set -Eeuo pipefail
    mkdir -p solr/data
    python3 ${params.dataload_home_inside_container}/08_create_other_dbs/solr/make_solr_config.py \
        --subgraph-name ${params.subgraph} \
        --in-graph-metadata-json ${graph_metadata_json} \
        --in-template-config-dir ${params.dataload_home_inside_container}/08_create_other_dbs/solr/solr_config_template \
        --out-config-dir ./solr/data
    python3 ${params.dataload_home_inside_container}/08_create_other_dbs/solr/solr_import.dockerpy \
       grebi_edges_${params.subgraph} 8986 ${params.solr_mem}
    """
}

process create_solr_autocomplete_core {
    cache "lenient"
    memory "4 GB" 
    time "4h"
    cpus "4"
    scratch "ram-disk"

    input:
    path(names_txt)

    output:
    path("solr/data/grebi_autocomplete_${params.subgraph}")

    script:
    """
    #!/usr/bin/env bash
    set -Eeuo pipefail
    mkdir -p solr/data
    python3 ${params.dataload_home_inside_container}/08_create_other_dbs/solr/make_solr_autocomplete_config.py \
        --subgraph-name ${params.subgraph} \
        --in-template-config-dir ${params.dataload_home_inside_container}/08_create_other_dbs/solr/solr_config_template \
        --out-config-dir ./solr/data
    python3 ${params.dataload_home_inside_container}/08_create_other_dbs/solr/solr_import.dockerpy \
        grebi_autocomplete_${params.subgraph} 8987 ${params.solr_mem}
    """
}

process create_solr_results_cores {
    cache "lenient"
    memory "4 GB" 
    time "4h"
    cpus "4"

    input:
    path(results_jsonl)

    output:
    path("solr/data/grebi_results__${params.subgraph}__${results_jsonl.simpleName}")

    script:
    """
    #!/usr/bin/env bash
    set -Eeuo pipefail
    mkdir -p solr/data
    python3 ${params.dataload_home_inside_container}/08_create_other_dbs/solr/make_solr_results_config.py \
        --subgraph-name ${params.subgraph} \
        --query-id ${results_jsonl.simpleName} \
        --in-template-config-dir ${params.dataload_home_inside_container}/08_create_other_dbs/solr/solr_config_template \
        --out-config-dir ./solr/data
    python3 ${params.dataload_home_inside_container}/08_create_other_dbs/solr/solr_import.dockerpy \
        grebi_results__${params.subgraph}__${results_jsonl.simpleName} 8987 ${params.solr_mem}
    """
}

process package_neo {
    cache "lenient"
    memory "4 GB" 
    time "8h"
    cpus "8"

    publishDir "${params.out}", overwrite: true

    input: 
    path("${params.subgraph}_neo4j")

    output:
    path("${params.subgraph}_neo4j.tgz")

    script:
    """
    tar -chf ${params.subgraph}_neo4j.tgz --use-compress-program="pigz --fast" ${params.subgraph}_neo4j
    """
}

process package_solr {
    cache "lenient"
    memory "4 GB" 
    time "8h"
    cpus "8"

    publishDir "${params.out}", overwrite: true

    input: 
    path(cores)

    output:
    path("${params.subgraph}_solr.tgz")

    script:
    """
    #!/usr/bin/env bash
    set -Eeuo pipefail
    cp -f ${params.dataload_home_inside_container}/08_create_other_dbs/solr/solr_config_template/*.xml .
    cp -f ${params.dataload_home_inside_container}/08_create_other_dbs/solr/solr_config_template/*.cfg .
    tar -chf ${params.subgraph}_solr.tgz --transform 's,^,solr/,' --use-compress-program="pigz --fast" \
	*.xml *.cfg ${cores}
    """
}

def parseJson(json) {
    return new JsonSlurper().parseText(json)
}

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
