
nextflow.enable.dsl=2

import groovy.json.JsonSlurper
import groovy.json.JsonOutput
import groovy.yaml.YamlSlurper

include { ingest } from './processes/01_ingest/ingest'
include { build_equiv_groups } from './processes/01_ingest/build_equiv_groups'
include { assign_ids } from './processes/02_assign_ids/assign_ids'
include { merge_ingests } from './processes/03_merge/merge_ingests'
include { index } from './processes/04_index/index'
include { link } from './processes/05_link/link'
include { merge_graph_metadata_jsons } from './processes/05_link/merge_graph_metadata_jsons'
include { create_compressed_blobs } from './processes/06_create_neo_db/create_compressed_blobs'
include { create_sqlite } from './processes/08_create_other_dbs/sqlite/create_sqlite'
include { prepare_neo } from './processes/06_create_neo_db/neo/prepare_neo'
include { create_neo_ids_csv } from './processes/06_create_neo_db/neo/create_neo_ids_csv'
include { create_neo } from './processes/06_create_neo_db/neo/create_neo'
include { package_neo } from './processes/06_create_neo_db/neo/package_neo'
include { prepare_solr } from './processes/08_create_other_dbs/solr/prepare_solr'
include { create_solr_nodes_core } from './processes/08_create_other_dbs/solr/create_solr_nodes_core'
include { create_solr_autocomplete_core } from './processes/08_create_other_dbs/solr/create_solr_autocomplete_core'
include { create_solr_results_cores } from './processes/08_create_other_dbs/solr/create_solr_results_cores'
include { construct_solr } from './processes/08_create_other_dbs/solr/construct_solr'
include { package_solr } from './processes/08_create_other_dbs/solr/package_solr'
include { prepare_postgres_edges } from './processes/08_create_other_dbs/postgres/prepare_postgres_edges'
include { prepare_postgres_nodes } from './processes/08_create_other_dbs/postgres/prepare_postgres_nodes'
include { create_postgres } from './processes/08_create_other_dbs/postgres/create_postgres'
include { package_postgres } from './processes/08_create_other_dbs/postgres/package_postgres'
include { run_materialised_queries } from './processes/07_run_queries/run_materialised_queries'
include { results_to_csv } from './processes/07_run_queries/results_to_csv'
include { link_results } from './processes/07_run_queries/link_results'
include { add_query_metadatas_to_graph_metadata } from './processes/07_run_queries/add_query_metadatas_to_graph_metadata'
include { csvs_to_sqlite } from './processes/07_run_queries/csvs_to_sqlite'
include { test_query_templates } from './processes/09_integration_tests/test_query_templates'
include { construct_release } from './processes/10_package_release/construct_release'
include { package_release } from './processes/10_package_release/package_release'

params.out = "$GREBI_OUT_DIR"
params.subgraph = "$GREBI_SUBGRAPH"
params.query_yamls_path = "$GREBI_QUERY_YAMLS_PATH"
params.solr_mem = "140g"
params.neo_mem = "140g"
params.neo_query_mem = "140g"
params.pg_shared_buffers = "2GB"
params.pg_work_mem = "256MB"
params.pg_maintenance_work_mem = "1GB"
params.pg_max_wal_size = "4GB"
params.pg_build_shared_buffers = "2GB"
params.pg_build_work_mem = "256MB"
params.pg_build_maintenance_work_mem = "1GB"
params.pg_build_max_wal_size = "4GB"
params.pg_build_effective_cache_size = "4GB"
params.integration_neo_heap = "512m"
params.integration_solr_heap = "512m"
params.integration_pg_shared_buffers = "128MB"
params.integration_pg_work_mem = "64MB"
params.integration_pg_maintenance_work_mem = "256MB"
params.integration_pg_max_wal_size = "1GB"
params.docker_image = "ghcr.io/ebispot/grebi_combined:dev"
params.dataload_home = "$GREBI_DATALOAD_HOME"
params.grebi_home = "$GREBI_HOME"
params.downloads_path = "$GREBI_DOWNLOADS_PATH"
params.export_snapshots = false

workflow {

    // Load subgraph configuration
    def sg_config = (new JsonSlurper().parse(new File(params.grebi_home, 'configs/subgraph_configs/' + params.subgraph + '.json')))

    // Load datasource configurations
    def datasources = sg_config.datasource_configs.collect { ds -> new YamlSlurper().parse(new File(params.grebi_home, ds)) }

    // Write resolved subgraph config with inline datasource configs (so they are loaded only once)
    def resolved_config = [:] + sg_config
    resolved_config.datasource_configs = datasources
    def resolved_config_path = "${workflow.workDir}/resolved_${params.subgraph}_subgraph_config.json"
    new File(resolved_config_path).text = JsonOutput.prettyPrint(JsonOutput.toJson(resolved_config))

    // Create channel of all datasource files
    // Globs are resolved relative to GREBI_DOWNLOADS_PATH
    datasource_files = Channel.from(datasources.collect {
        ds -> ds.ingests.collect {
            ingest -> ingest.globs.collect {
                glob -> files("${params.downloads_path}/${glob}").collect {
                    file -> [
                        datasource: ds,
                        ingest: ingest,
                        filename: file.toString()
                    ]
                }
            }
        }
     }) | flatten

    // === STEP 1: INGEST ===
    ingest(
        datasource_files, 
        Channel.value(sg_config.identifier_props), 
        Channel.value(sg_config.bytes_per_merged_file)
    )

    // Build equivalence groups from identifiers
    groups_txt = build_equiv_groups(
        ingest.out.identifiers.collect(), 
        Channel.value(sg_config.additional_equivalence_groups)
    )

    // === STEP 2: ASSIGN IDS ===
    nodes_for_assign = ingest.out.nodes.flatMap { datasource_name, files ->
        def fs = (files instanceof List ? files : [files])
        fs.collect { f -> tuple(datasource_name, f) }
    }

    assigned = assign_ids(
        nodes_for_assign, 
        groups_txt, 
        Channel.value(sg_config.identifier_props), 
        Channel.value(sg_config.type_superclasses)
    ).collect(flat: false)

    // === STEP 3: MERGE ===
    merged = merge_ingests(
        assigned,
        Channel.value(sg_config.exclude_props),
        Channel.value(sg_config.prioritise_datasources),
        Channel.value(sg_config.bytes_per_merged_file),
        Channel.value(params.subgraph)
    )

    // === STEP 4: INDEX ===
    indexed = index(merged.collect(), Channel.value(params.subgraph), Channel.value(resolved_config_path))

    // === STEP 5: LINK ===
    link(
        merged.flatten(), 
        indexed.entity_metadata_jsonl, 
        indexed.graph_metadata_json, 
        Channel.value(sg_config.exclude_edges + sg_config.identifier_props), 
        Channel.value(sg_config.exclude_self_referential_edges + sg_config.identifier_props), 
        groups_txt
    )
    
    merge_graph_metadata_jsons(
        indexed.graph_metadata_json.collect() + link.out.linked_summary.collect(),
        Channel.value(params.subgraph),
        Channel.value(params.downloads_path)
    )

    // === STEP 6: CREATE DATABASES ===
    
    // SQLite
    compressed_blobs = create_compressed_blobs(link.out.nodes.mix(link.out.edges), Channel.value(params.subgraph))
    sqlite = create_sqlite(compressed_blobs.collect(), Channel.value(params.subgraph), Channel.value(params.out))

    // Neo4j
    neo_input_dir = prepare_neo(
        indexed.graph_metadata_json, 
        link.out.nodes, 
        link.out.edges,
        Channel.value(params.subgraph)
    )

    ids_csv = create_neo_ids_csv(indexed.ids_txt, Channel.value(params.subgraph))
    
    neo_db = create_neo(
        prepare_neo.out.nodes.collect() +
        prepare_neo.out.edges.collect() +
        prepare_neo.out.id_edges.collect() +
        ids_csv.collect(),
        Channel.value(params.subgraph),
        Channel.value(params.neo_mem)
    )

    // === STEP 7: RUN QUERIES ===
    run_materialised_queries(
        neo_db, 
        params.query_yamls_path,
        Channel.value(params.subgraph),
        Channel.value(params.neo_query_mem),
        Channel.value(params.out)
    )

    csv_results = results_to_csv(
        run_materialised_queries.out.results.flatten(),
        Channel.value(params.out)
    )
    linked_results = link_results(
        run_materialised_queries.out.results.flatten(), 
        indexed.entity_metadata_jsonl, 
        groups_txt
    )

    add_query_metadatas_to_graph_metadata(
        run_materialised_queries.out.metadata.flatten().collect(), 
        merge_graph_metadata_jsons.out,
        Channel.value(params.subgraph),
        Channel.value(params.out)
    )

    // === STEP 8: CREATE SOLR CORES ===
    solr_inputs = prepare_solr(link.out.nodes, Channel.value(params.subgraph))
    
    solr_nodes_core = create_solr_nodes_core(
        prepare_solr.out.nodes.collect(), 
        indexed.names_txt, 
        merge_graph_metadata_jsons.out,
        Channel.value(params.subgraph),
        Channel.value(params.solr_mem)
    )
    
    solr_autocomplete_core = create_solr_autocomplete_core(
        indexed.names_txt,
        Channel.value(params.subgraph),
        Channel.value(params.solr_mem)
    )
    
    solr_results_cores = create_solr_results_cores(
        linked_results,
        Channel.value(params.subgraph),
        Channel.value(params.solr_mem)
    )

    all_solr_cores = solr_nodes_core
        .concat(solr_autocomplete_core)
        .concat(solr_results_cores)
        .collect()

    // === STEP 8b: CREATE POSTGRESQL ===
    postgres_edge_inputs = prepare_postgres_edges(link.out.edges, indexed.graph_metadata_json, Channel.value(params.subgraph))
    postgres_node_inputs = prepare_postgres_nodes(link.out.nodes, link.out.linked_summary, Channel.value(params.subgraph))

    postgres_db = create_postgres(
        prepare_postgres_edges.out.edges_tsv.collect(),
        prepare_postgres_edges.out.columns.collect(),
        prepare_postgres_nodes.out.nodes_tsv.collect(),
        prepare_postgres_nodes.out.columns.collect(),
        Channel.value(params.subgraph)
    )

    // === PACKAGE OUTPUTS ===
    solr_dir = construct_solr(all_solr_cores)
    solr_tgz = package_solr(solr_dir, Channel.value(params.subgraph), Channel.value(params.out))
    neo_tgz = package_neo(neo_db, Channel.value(params.subgraph), Channel.value(params.out))
    postgres_tgz = package_postgres(postgres_db, Channel.value(params.subgraph), Channel.value(params.out))

    // === CONSTRUCT & PACKAGE RELEASE ===
    release_dir = construct_release(
        neo_db,
        solr_dir,
        postgres_db,
        sqlite,
        add_query_metadatas_to_graph_metadata.out,
        Channel.fromPath("${params.grebi_home}/query_templates"),
        Channel.value(params.subgraph),
        Channel.value(params.out),
        Channel.value(params.docker_image),
        Channel.value(params.dataload_home)
    )

    release_tgz = package_release(
        release_dir,
        Channel.value(params.subgraph),
        Channel.value(params.out)
    )

    // === RUN INTEGRATION TESTS ===
    test_query_templates(
        release_tgz,
        Channel.value(params.subgraph),
        Channel.value(params.out),
        Channel.value(params.export_snapshots),
        Channel.value(params.grebi_home),
        Channel.value(params.integration_neo_heap),
        Channel.value(params.integration_solr_heap),
        Channel.value(params.integration_pg_shared_buffers),
        Channel.value(params.integration_pg_work_mem),
        Channel.value(params.integration_pg_maintenance_work_mem),
        Channel.value(params.integration_pg_max_wal_size)
    )
}

// Utility functions
def parseJson(json) {
    return new JsonSlurper().parseText(json)
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
