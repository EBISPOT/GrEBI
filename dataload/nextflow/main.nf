
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
params.subgraphs = "$GREBI_SUBGRAPHS"
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
params.make_docs = false

workflow {

    def subgraph_names = params.subgraphs.tokenize(',')

    // Load all subgraph and datasource configurations
    def configs = [:]
    def resolved_paths = [:]
    subgraph_names.each { sg ->
        def sg_config = new JsonSlurper().parse(new File(params.grebi_home, "configs/subgraph_configs/${sg}.json"))
        def datasources = sg_config.datasource_configs.collect { ds -> new YamlSlurper().parse(new File(params.grebi_home, ds)) }
        def resolved = [:] + sg_config
        resolved.datasource_configs = datasources
        def rpath = "${workflow.workDir}/resolved_${sg}_subgraph_config.json"
        new File(rpath).text = JsonOutput.prettyPrint(JsonOutput.toJson(resolved))
        configs[sg] = [sg_config: sg_config, datasources: datasources]
        resolved_paths[sg] = rpath
    }

    // Create channel of all datasource files, tagged with subgraph
    // Each item: [sg, file_listing, identifier_props, bytes_per_merged_file]
    datasource_files = Channel.from(
        subgraph_names.collectMany { sg ->
            def cfg = configs[sg]
            cfg.datasources.collectMany { ds ->
                ds.ingests.collectMany { ingest_spec ->
                    ingest_spec.globs.collectMany { glob ->
                        files("${params.downloads_path}/${sg}/${glob}").collect { f ->
                            [sg,
                             [datasource: ds, ingest: ingest_spec, filename: f.toString()],
                             cfg.sg_config.identifier_props,
                             cfg.sg_config.bytes_per_merged_file]
                        }
                    }
                }
            }
        }
    )

    // === STEP 1: INGEST ===
    ingest(datasource_files)
    // ingest.out.nodes: [sg, ds_id, node_files]
    // ingest.out.identifiers: [sg, identifiers_file]

    // Gather identifiers per subgraph, combine with equiv group config
    equiv_config_ch = Channel.from(
        subgraph_names.collect { sg -> [sg, configs[sg].sg_config.additional_equivalence_groups] }
    )
    build_equiv_input = ingest.out.identifiers
        .groupTuple(by: 0)
        .combine(equiv_config_ch, by: 0)
    // → [sg, [ident_files...], additional_equiv_groups]

    groups = build_equiv_groups(build_equiv_input)
    // groups: [sg, groups.txt]

    // === STEP 2: ASSIGN IDS ===
    // Flatten nodes to per-file tuples: [sg, ds_id, single_file]
    nodes_flat = ingest.out.nodes.flatMap { sg, ds_id, node_files ->
        def fs = (node_files instanceof List ? node_files : [node_files])
        fs.collect { f -> [sg, ds_id, f] }
    }

    // Combine with groups and config
    assign_config_ch = Channel.from(
        subgraph_names.collect { sg ->
            [sg, configs[sg].sg_config.identifier_props, configs[sg].sg_config.type_superclasses]
        }
    )
    assign_input = nodes_flat
        .combine(groups, by: 0)
        .combine(assign_config_ch, by: 0)
    // → [sg, ds_id, file, groups.txt, identifier_props, type_superclasses]

    assigned = assign_ids(assign_input)
    // assigned: [sg, ds_name, assigned_file]

    // === STEP 3: MERGE ===
    // Collect assigned per subgraph, reconstruct as list of [ds_name, file] pairs
    assigned_grouped = assigned
        .map { sg, ds_name, f -> [sg, [ds_name, f]] }
        .groupTuple(by: 0)
    // → [sg, [[ds1, f1], [ds2, f2], ...]]

    merge_config_ch = Channel.from(
        subgraph_names.collect { sg ->
            [sg, configs[sg].sg_config.exclude_props,
             configs[sg].sg_config.prioritise_datasources,
             configs[sg].sg_config.bytes_per_merged_file]
        }
    )
    merge_input = assigned_grouped
        .combine(merge_config_ch, by: 0)
    // → [sg, [[ds1,f1],...], exclude_props, prioritise_ds, bytes_per_merged]

    merged = merge_ingests(merge_input)
    // merged: [sg, [merged.jsonl.*]]

    // === STEP 4: INDEX ===
    index_config_ch = Channel.from(
        subgraph_names.collect { sg -> [sg, resolved_paths[sg]] }
    )
    index_input = merged
        .combine(index_config_ch, by: 0)
    // → [sg, merged_files, config_path]

    indexed = index(index_input)
    // indexed.entity_metadata_jsonl: [sg, entity_metadata.jsonl]
    // indexed.graph_metadata_json: [sg, graph_metadata.json]
    // indexed.names_txt: [sg, names.txt]
    // indexed.ids_txt: [sg, ids_sg.txt]

    // === STEP 5: LINK ===
    // Flatten merged files to per-file tuples for scattered processing
    merged_flat = merged.flatMap { sg, merge_files ->
        def fs = (merge_files instanceof List ? merge_files : [merge_files])
        fs.collect { f -> [sg, f] }
    }

    // Per-subgraph exclude config
    link_config_ch = Channel.from(
        subgraph_names.collect { sg ->
            def cfg = configs[sg].sg_config
            [sg, cfg.exclude_edges + cfg.identifier_props,
             cfg.exclude_self_referential_edges + cfg.identifier_props]
        }
    )

    // Combine all per-subgraph context with each merged file
    link_input = merged_flat
        .combine(indexed.entity_metadata_jsonl, by: 0)
        .combine(indexed.graph_metadata_json, by: 0)
        .combine(link_config_ch, by: 0)
        .combine(groups, by: 0)
    // → [sg, merged_file, entity_meta, graph_meta, exclude, exclude_self_ref, groups]

    link(link_input)
    // link.out.nodes: [sg, linked_nodes_file]
    // link.out.edges: [sg, linked_edges_file]
    // link.out.linked_summary: [sg, linked_summary_file]

    // Merge graph metadata (per-subgraph gather)
    graph_metas_for_merge = indexed.graph_metadata_json
        .mix(link.out.linked_summary)
        .groupTuple(by: 0)
        .map { sg, meta_files -> [sg, meta_files, "${params.downloads_path}/${sg}"] }
    // → [sg, [graph_meta, summary1, ...], downloads_path_for_sg]

    merge_graph_metadata_jsons(graph_metas_for_merge)
    // → [sg, merged_metadata.json]

    // === STEP 6: CREATE DATABASES ===

    // SQLite (per-subgraph)
    compressed_blobs = create_compressed_blobs(
        link.out.nodes.mix(link.out.edges)
    )
    // compressed_blobs: [sg, blob_file]

    blobs_grouped = compressed_blobs.groupTuple(by: 0)
    // → [sg, [blob1, blob2, ...]]

    sqlite = create_sqlite(blobs_grouped, Channel.value(params.out))
    // sqlite: [sg, subgraph.sqlite3]

    // Neo4j (per-subgraph)
    // Pair nodes+edges from same link invocation using positional merge
    link_nodes_edges = link.out.nodes.merge(link.out.edges)
        .map { sg1, nodes, sg2, edges -> [sg1, nodes, edges] }

    prepare_neo_input = link_nodes_edges
        .combine(indexed.graph_metadata_json, by: 0)
        .map { sg, nodes, edges, graph_meta -> [sg, graph_meta, nodes, edges] }
    // → [sg, graph_meta, nodes, edges]

    prepare_neo(prepare_neo_input)

    ids_csv = create_neo_ids_csv(indexed.ids_txt)

    // Collect all neo CSVs per subgraph
    neo_csvs_per_sg = prepare_neo.out.nodes
        .mix(prepare_neo.out.edges)
        .mix(prepare_neo.out.id_edges)
        .mix(ids_csv)
        .groupTuple(by: 0)
    // → [sg, [csv1, csv2, ...]]

    neo_db = create_neo(neo_csvs_per_sg, Channel.value(params.neo_mem))
    // neo_db: [sg, sg_neo4j/]

    // === STEP 7: RUN QUERIES (per-subgraph) ===
    queries_input = neo_db
        .map { sg, neo -> [sg, neo, file(params.query_yamls_path)] }
    // → [sg, neo_dir, query_yamls_path]

    run_materialised_queries(
        queries_input,
        Channel.value(params.neo_query_mem),
        Channel.value(params.out)
    )
    // .metadata: [sg, queries.json]
    // .results: [sg, [*.results.jsonl]]
    // .metadatas: [sg, [*.json]]

    // Flatten results to per-file
    results_flat = run_materialised_queries.out.results
        .flatMap { sg, result_files ->
            def fs = (result_files instanceof List ? result_files : [result_files])
            fs.collect { f -> [sg, f] }
        }

    csv_results = results_to_csv(results_flat, Channel.value(params.out))

    // link_results needs [sg, results_file, entity_metadata, groups]
    link_results_input = results_flat
        .combine(indexed.entity_metadata_jsonl, by: 0)
        .combine(groups, by: 0)
    // → [sg, results_file, entity_meta, groups]

    linked_results = link_results(link_results_input)
    // linked_results: [sg, linked_results.jsonl]

    // Add query metadata to graph metadata (per-subgraph)
    add_meta_input = run_materialised_queries.out.metadata
        .combine(merge_graph_metadata_jsons.out, by: 0)
    // → [sg, queries.json, merged_graph_meta]

    add_query_metadatas_to_graph_metadata(
        add_meta_input,
        Channel.value(params.out)
    )
    // → [sg, sg_metadata.json]

    // === STEP 8: CREATE SOLR CORES (per-subgraph, then cross-subgraph gather) ===
    prepare_solr(link.out.nodes)

    solr_nodes_input = prepare_solr.out.nodes
        .groupTuple(by: 0)
        .combine(indexed.names_txt, by: 0)
        .combine(merge_graph_metadata_jsons.out, by: 0)
    // → [sg, [solr_nodes_files], names.txt, merged_meta.json]

    solr_nodes_core = create_solr_nodes_core(
        solr_nodes_input,
        Channel.value(params.solr_mem)
    )

    solr_autocomplete_core = create_solr_autocomplete_core(
        indexed.names_txt,
        Channel.value(params.solr_mem)
    )

    solr_results_cores = create_solr_results_cores(
        linked_results,
        Channel.value(params.solr_mem)
    )

    // Strip subgraph tags and collect ALL cores for cross-subgraph solr
    all_solr_cores = solr_nodes_core
        .map { sg, core -> core }
        .mix(solr_autocomplete_core.map { sg, core -> core })
        .mix(solr_results_cores.map { sg, core -> core })
        .collect()

    solr_dir = construct_solr(all_solr_cores)

    // === STEP 8b: CREATE POSTGRESQL (cross-subgraph) ===
    // Pair edges with graph metadata for prepare_postgres_edges
    pg_edges_input = link.out.edges
        .combine(indexed.graph_metadata_json, by: 0)
    // → [sg, edges_file, graph_meta]

    prepare_postgres_edges(pg_edges_input)

    // Pair nodes with linked_summary for prepare_postgres_nodes
    pg_nodes_input = link.out.nodes.merge(link.out.linked_summary)
        .map { sg1, nodes, sg2, summary -> [sg1, nodes, summary] }
    // → [sg, nodes_file, linked_summary]

    prepare_postgres_nodes(pg_nodes_input)

    // Strip subgraph tags and collect ALL files for cross-subgraph postgres
    postgres_db = create_postgres(
        prepare_postgres_edges.out.edges_tsv.map { sg, f -> f }.collect(),
        prepare_postgres_edges.out.columns.map { sg, f -> f }.collect(),
        prepare_postgres_nodes.out.nodes_tsv.map { sg, f -> f }.collect(),
        prepare_postgres_nodes.out.columns.map { sg, f -> f }.collect()
    )

    // === PACKAGE OUTPUTS ===
    solr_tgz = package_solr(solr_dir, Channel.value(params.out))
    neo_tgz = package_neo(neo_db, Channel.value(params.out))
    postgres_tgz = package_postgres(postgres_db, Channel.value(params.out))

    // === CONSTRUCT & PACKAGE RELEASE ===
    release_dir = construct_release(
        neo_db.map { sg, neo -> neo }.collect(),
        solr_dir,
        postgres_db,
        sqlite.map { sg, s -> s }.collect(),
        add_query_metadatas_to_graph_metadata.out.map { sg, meta -> meta }.collect(),
        Channel.fromPath("${params.grebi_home}/query_templates"),
        Channel.value(params.subgraphs),
        Channel.value(params.out),
        Channel.value(params.docker_image),
        Channel.value(params.dataload_home)
    )

    release_tgz = package_release(
        release_dir,
        Channel.value(params.out)
    )

    // === RUN INTEGRATION TESTS ===
    test_query_templates(
        release_tgz,
        Channel.value(params.subgraphs),
        Channel.value(params.out),
        Channel.value(params.export_snapshots),
        Channel.value(params.make_docs),
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
