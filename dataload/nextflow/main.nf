
nextflow.enable.dsl=2

import groovy.json.JsonSlurper
import groovy.json.JsonOutput
import groovy.yaml.YamlSlurper

include { ingest } from './processes/01_ingest/ingest'
include { build_ubergraph } from './processes/01_ingest/build_ubergraph'
include { build_equiv_groups } from './processes/01_ingest/build_equiv_groups'
include { assign_ids } from './processes/02_assign_ids/assign_ids'
include { merge_ingests } from './processes/03_merge/merge_ingests'
include { index } from './processes/04_index/index'
include { link } from './processes/05_link/link'
include { merge_graph_metadata_jsons } from './processes/05_link/merge_graph_metadata_jsons'
include { create_compressed_blobs } from './processes/06_create_neo_db/create_compressed_blobs'
include { prepare_postgres_blobs } from './processes/08_create_postgres/prepare_postgres_blobs'
include { prepare_neo } from './processes/06_create_neo_db/neo/prepare_neo'
include { create_neo_ids_csv } from './processes/06_create_neo_db/neo/create_neo_ids_csv'
include { create_neo } from './processes/06_create_neo_db/neo/create_neo'
include { package_neo } from './processes/06_create_neo_db/neo/package_neo'
include { prepare_postgres_edges } from './processes/08_create_postgres/prepare_postgres_edges'
include { prepare_postgres_autocomplete } from './processes/08_create_postgres/prepare_postgres_autocomplete'
include { prepare_postgres_mat_queries } from './processes/08_create_postgres/prepare_postgres_mat_queries'
include { prepare_postgres_nodes } from './processes/08_create_postgres/prepare_postgres_nodes'
include { create_postgres } from './processes/08_create_postgres/create_postgres'
include { populate_external_postgres } from './processes/08_create_postgres/populate_external_postgres'
include { package_postgres } from './processes/08_create_postgres/package_postgres'
include { run_materialised_queries } from './processes/07_run_queries/run_materialised_queries'
include { results_to_csv } from './processes/07_run_queries/results_to_csv'
include { link_results } from './processes/07_run_queries/link_results'
include { add_query_metadatas_to_graph_metadata } from './processes/07_run_queries/add_query_metadatas_to_graph_metadata'
include { test_query_templates } from './processes/09_integration_tests/test_query_templates'
include { render_docs_pdf } from './processes/09_integration_tests/render_docs_pdf'
include { construct_release } from './processes/10_package_release/construct_release'
include { package_release } from './processes/10_package_release/package_release'

params.out = "$GREBI_OUT_DIR"
params.subgraphs = "$GREBI_SUBGRAPHS"
params.query_yamls_path = "$GREBI_QUERY_YAMLS_PATH"
params.neo_mem = "140g"
params.neo_query_mem = "140g"
params.pg_shared_buffers = "2GB"
params.pg_work_mem = "256MB"
params.pg_maintenance_work_mem = "1GB"
// Index-build maintenance_work_mem for the external Postgres specifically; it
// sizes the parallel-build shared memory segment and that server is not ours to
// resize. Defaulted here so it is never undefined — populate_external_postgres
// tests it with a ternary and would silently omit the setting otherwise.
params.external_pg_maintenance_work_mem = "1GB"
params.pg_parallel_workers = 2
params.pg_max_wal_size = "4GB"
params.pg_build_shared_buffers = "2GB"
params.pg_build_work_mem = "256MB"
params.pg_build_maintenance_work_mem = "1GB"
params.pg_build_max_wal_size = "4GB"
params.pg_build_effective_cache_size = "4GB"
params.integration_neo_heap = "512m"
params.integration_pg_shared_buffers = "128MB"
params.integration_pg_work_mem = "64MB"
params.integration_pg_maintenance_work_mem = "256MB"
params.integration_pg_max_wal_size = "1GB"
params.docker_image = "ghcr.io/ebispot/grebi_combined:dev"
params.dataload_home = "$GREBI_DATALOAD_HOME"
params.grebi_home = "$GREBI_HOME"
params.downloads_path = "$GREBI_DOWNLOADS_PATH"
params.external_postgres = false
params.export_snapshots = false
params.make_docs = false
// The standalone postgres.tar.xz artifact (not part of release.tar.xz —
// construct_release packages postgres_data directly). Disabled on codon for
// now: the tarball put the scratch over its /hps/nobackup disk quota and
// nothing downstream consumes it yet.
params.package_postgres = true

workflow {

    def subgraph_names = params.subgraphs.tokenize(',')

    // Load all subgraph and datasource configurations
    def configs = [:]
    def resolved_paths = [:]
    subgraph_names.each { sg ->
        def sg_config = new JsonSlurper().parse(new File(params.grebi_home, "configs/subgraph_configs/${sg}.json"))
        def datasources = sg_config.datasource_configs.collect { ds -> new YamlSlurper().parse(new File(params.grebi_home, ds)) }
        def ontology_sets = (sg_config.ontology_sets ?: []).collect { os -> new YamlSlurper().parse(new File(params.grebi_home, os)) }
        def resolved = [:] + sg_config
        resolved.datasource_configs = datasources
        resolved.ontology_sets = ontology_sets
        def rpath = "${workflow.workDir}/resolved_${sg}_subgraph_config.json"
        new File(rpath).text = JsonOutput.prettyPrint(JsonOutput.toJson(resolved))
        configs[sg] = [sg_config: sg_config, datasources: datasources, ontology_sets: ontology_sets]
        resolved_paths[sg] = rpath
    }

    // Create channel of all datasource files, tagged with subgraph
    // Each item: [sg, file_listing, identifier_props, bytes_per_merged_file]
    datasource_files = Channel.from(
        subgraph_names.collectMany { sg ->
            def cfg = configs[sg]
            cfg.datasources.findAll { !it.from_ubergraph }.collectMany { ds ->
                ds.ingests.collectMany { ingest_spec ->
                    ingest_spec.globs.collectMany { glob ->
                        // files() returns a no-wildcard path even if it doesn't
                        // exist, so skip missing/empty inputs (e.g. optional
                        // sources that weren't downloaded).
                        files("${params.downloads_path}/${sg}/${glob}").findAll { it.exists() && it.size() > 0 }.collect { f ->
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

    // === STEP 0: BUILD ONTOLOGY UBERGRAPHS ===
    // Each ontology_set is built by `om ubergraph` into one ubergraph.nq.gz.
    // Nextflow runs these (slow, reasoning-heavy) builds in parallel with the
    // normal datasource ingests below.
    ubergraph_sets = Channel.from(
        subgraph_names.collectMany { sg ->
            configs[sg].ontology_sets.collect { os -> [sg, os, params.grebi_home, params.downloads_path] }
        }
    )
    built_ubergraphs = build_ubergraph(ubergraph_sets)
    // built_ubergraphs: [sg, set_id, nq_path]

    // Feed the built ubergraph nq into the ingest step for the subgraph's
    // from_ubergraph datasources (Ontologies / .redundant / .nonredundant).
    ubergraph_files = built_ubergraphs.flatMap { sg, set_id, nq ->
        def cfg = configs[sg]
        cfg.datasources.findAll { it.from_ubergraph }.collectMany { ds ->
            ds.ingests.collect { ingest_spec ->
                [sg,
                 [datasource: ds, ingest: ingest_spec, filename: nq.toString()],
                 cfg.sg_config.identifier_props,
                 cfg.sg_config.bytes_per_merged_file]
            }
        }
    }

    // === STEP 1: INGEST ===
    ingest(datasource_files.mix(ubergraph_files))
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
        .map { sg, assigned_files -> [sg, sortAssignedEntries(assigned_files)] }
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
        .map { sg, merge_files -> [sg, sortPaths(merge_files)] }
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
        sortPaths(merge_files).collect { f -> [sg, mergedShardId(f), f] }
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
    // → [sg, shard_id, merged_file, entity_meta, graph_meta, exclude, exclude_self_ref, groups]

    link(link_input)
    // link.out.nodes: [sg, linked_nodes_file]
    // link.out.edges: [sg, linked_edges_file]
    // link.out.linked_summary: [sg, linked_summary_file]

    // Merge graph metadata (per-subgraph gather)
    graph_metas_for_merge = indexed.graph_metadata_json
        .mix(link.out.linked_summary.map { sg, shard_id, summary -> [sg, summary] })
        .groupTuple(by: 0)
        .map { sg, meta_files -> [sg, sortPaths(meta_files), "${params.downloads_path}/${sg}"] }
    // → [sg, [graph_meta, summary1, ...], downloads_path_for_sg]

    merge_graph_metadata_jsons(graph_metas_for_merge)
    // → [sg, merged_metadata.json]

    // === STEP 6: CREATE DATABASES ===

    // Compressed blobs (per-shard, then converted to PG COPY BINARY)
    compressed_blobs = create_compressed_blobs(
        link.out.nodes
            .map { sg, shard_id, f -> [sg, "nodes", shard_id, f] }
            .mix(link.out.edges.map { sg, shard_id, f -> [sg, "edges", shard_id, f] })
    )
    // compressed_blobs: [sg, blob_kind, shard_id, blob_file]

    postgres_blobs = prepare_postgres_blobs(compressed_blobs)
    // postgres_blobs.blobs_pgbin: [sg, blob_kind, shard_id, pgbin_file]

    // Neo4j (per-subgraph)
    // Pair nodes+edges from the same merged shard using stable keys
    link_nodes_edges = link.out.nodes
        .join(link.out.edges, by: [0, 1])
        .map { sg, shard_id, nodes, edges -> [sg, shard_id, nodes, edges] }

    prepare_neo_input = link_nodes_edges
        .combine(indexed.graph_metadata_json, by: 0)
        .map { sg, shard_id, nodes, edges, graph_meta -> [sg, shard_id, graph_meta, nodes, edges] }
    // → [sg, shard_id, graph_meta, nodes, edges]

    prepare_neo(prepare_neo_input)

    ids_csv = create_neo_ids_csv(indexed.ids_txt)

    // Collect all neo CSVs per subgraph
    neo_csvs_per_sg = prepare_neo.out.nodes
        .map { sg, shard_id, f -> [sg, f] }
        .mix(prepare_neo.out.edges.map { sg, shard_id, f -> [sg, f] })
        .mix(prepare_neo.out.id_edges.map { sg, shard_id, f -> [sg, f] })
        .mix(ids_csv)
        .groupTuple(by: 0)
        .map { sg, neo_inputs -> [sg, sortPaths(neo_inputs)] }
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
            sortPaths(result_files).collect { f -> [sg, f] }
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

    // === STEP 8: PREPARE AUTOCOMPLETE + MATERIALISED QUERY DATA FOR POSTGRES ===
    prepare_postgres_autocomplete(indexed.names_txt)
    // autocomplete_pgbin: [sg, autocomplete_pgbin]

    // Pair each query's linked results with its metadata json (which names the
    // storage table + typed columns), keyed on [sg, query_id] via filenames:
    // {qid}.linked_results.jsonl / {qid}.json. queries.json is the per-subgraph
    // list, not a per-query file.
    mat_query_metas = run_materialised_queries.out.metadatas
        .flatMap { sg, files ->
            (files instanceof List ? files : [files])
                .findAll { f -> f.simpleName != 'queries' }
                .collect { f -> [[sg, f.simpleName], f] }
        }

    prepare_mat_queries_input = linked_results
        .map { sg, f -> [[sg, f.simpleName], f] }
        .combine(mat_query_metas, by: 0)
        .map { key, linked, meta -> [key[0], linked, meta] }
    // → [sg, linked_results.jsonl, {qid}.json]

    prepare_postgres_mat_queries(prepare_mat_queries_input)
    // mat_queries_pgbin/columns/indexes: [sg, matq_{sg}_{qid}.*]

    // === STEP 8b: CREATE POSTGRESQL (cross-subgraph) ===
    // Pair edges with graph metadata for prepare_postgres_edges
    pg_edges_input = link.out.edges
        .combine(indexed.graph_metadata_json, by: 0)
    // → [sg, shard_id, edges_file, graph_meta]

    prepare_postgres_edges(pg_edges_input)

    // Pair nodes with merged graph metadata (has all embedding models)
    pg_nodes_input = link.out.nodes
        .combine(merge_graph_metadata_jsons.out, by: 0)
    // → [sg, shard_id, nodes_file, merged_graph_metadata]

    prepare_postgres_nodes(pg_nodes_input)

    // Strip subgraph tags and collect ALL files for cross-subgraph postgres
    all_edges_pgbins = prepare_postgres_edges.out.edges_pgbin
        .map { sg, shard_id, f -> f }
        .collect()
        .map { files -> sortPaths(files) }
    all_edges_cols = prepare_postgres_edges.out.columns
        .map { sg, shard_id, f -> f }
        .collect()
        .map { files -> sortPaths(files) }
    all_nodes_pgbins = prepare_postgres_nodes.out.nodes_pgbin
        .map { sg, shard_id, f -> f }
        .collect()
        .map { files -> sortPaths(files) }
    all_nodes_cols = prepare_postgres_nodes.out.columns
        .map { sg, shard_id, f -> f }
        .collect()
        .map { files -> sortPaths(files) }
    all_blobs_pgbins = postgres_blobs.blobs_pgbin
        .map { sg, blob_kind, shard_id, f -> f }
        .collect()
        .map { files -> sortPaths(files) }
    all_autocomplete_pgbins = prepare_postgres_autocomplete.out.autocomplete_pgbin
        .map { sg, f -> f }
        .collect()
        .map { files -> sortPaths(files) }
    all_mat_queries_pgbins = prepare_postgres_mat_queries.out.mat_queries_pgbin
        .map { sg, f -> f }
        .collect()
        .map { files -> sortPaths(files) }
    all_mat_queries_cols = prepare_postgres_mat_queries.out.columns
        .map { sg, f -> f }
        .collect()
        .map { files -> sortPaths(files) }
    all_mat_queries_indexes = prepare_postgres_mat_queries.out.indexes
        .map { sg, f -> f }
        .collect()
        .map { files -> sortPaths(files) }
    all_metadata_jsons = add_query_metadatas_to_graph_metadata.out
        .map { sg, meta -> meta }
        .collect()
        .map { files -> sortPaths(files) }

    // create_postgres always runs (produces the packaged release artifact)
    postgres_db = create_postgres(
        all_edges_pgbins, all_edges_cols,
        all_nodes_pgbins, all_nodes_cols,
        all_blobs_pgbins,
        all_autocomplete_pgbins,
        all_mat_queries_pgbins, all_mat_queries_cols, all_mat_queries_indexes,
        all_metadata_jsons
    )

    // populate_external_postgres is optional, runs in addition to create_postgres
    if (params.external_postgres) {
        populate_external_postgres(
            all_edges_pgbins, all_edges_cols,
            all_nodes_pgbins, all_nodes_cols,
            all_blobs_pgbins,
            all_autocomplete_pgbins,
            all_mat_queries_pgbins, all_mat_queries_cols, all_mat_queries_indexes,
            all_metadata_jsons
        )
    }

    // === PACKAGE OUTPUTS ===
    neo_tgz = package_neo(neo_db, Channel.value(params.out))
    if (params.package_postgres) {
        package_postgres(postgres_db, Channel.value(params.out))
    }

    // === CONSTRUCT & PACKAGE RELEASE ===
    release_dir = construct_release(
        neo_db.map { sg, neo -> neo }.collect(),
        postgres_db,
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
    // Runs the stack straight out of the release directory rather than
    // re-extracting the tarball (a multi-hour, multi-TB round trip at codon
    // scale). Booting Neo4j/Postgres mutates the stores, which is fine once
    // the tarball — the actual release artifact — has been written: release_tgz
    // is passed purely as an ordering dependency so the test cannot start
    // before package_release has finished reading the directory.
    test_query_templates(
        release_dir,
        release_tgz,
        Channel.value(params.subgraphs),
        Channel.value(params.out),
        Channel.value(params.export_snapshots),
        Channel.value(params.make_docs),
        Channel.value(params.grebi_home),
        Channel.value(params.integration_neo_heap),
        Channel.value(params.integration_pg_shared_buffers),
        Channel.value(params.integration_pg_work_mem),
        Channel.value(params.integration_pg_maintenance_work_mem),
        Channel.value(params.integration_pg_max_wal_size)
    )

    // === RENDER DOCS PDF ===
    // Only fires when --make_docs produced grebi-docs.html. Runs in the
    // upstream puppeteer image so chromium stays out of the GrEBI images.
    render_docs_pdf(
        test_query_templates.out.docs_html,
        Channel.fromPath("${params.grebi_home}/webapp/render_pdf.mjs"),
        Channel.value(params.out)
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
    sortAssignedEntries(assigned).each { a ->
        res += "${a[0]}:${a[1]} "
    }
    return res
}

def sortAssignedEntries(assigned) {
    def entries = assigned instanceof List ? assigned : [assigned]
    // toSorted returns a new list; an in-place .sort mutates the Nextflow-owned
    // channel list, which is shared across parallel operators and triggers a
    // ConcurrentModificationException.
    entries.toSorted { a, b ->
        def left = "${a[0]}\u0000${a[1]}"
        def right = "${b[0]}\u0000${b[1]}"
        left <=> right
    }
}

def sortPaths(paths) {
    def values = paths instanceof List ? paths : [paths]
    // Non-mutating sort: see sortAssignedEntries — avoids mutating the shared
    // Nextflow channel list (ConcurrentModificationException under parallelism).
    values.toSorted { a, b -> basename(a.toString()) <=> basename(b.toString()) }
}

def mergedShardId(pathLike) {
    def name = basename(pathLike.toString())
    if (!name.startsWith("merged.jsonl.")) {
        throw new IllegalArgumentException("Unexpected merged shard filename: ${name}")
    }
    name.substring("merged.jsonl.".length())
}

def basename(filename) {
    return new File(filename).name
}
