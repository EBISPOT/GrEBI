
// Build an owlmake ubergraph from an ontology set, as part of dataload.
//
// Runs `om ubergraph` over the set's OWL sources -> one ubergraph.nq.gz written
// into the downloads path. The output is fed into the ingest step for the
// from_ubergraph datasources (Ontologies / .redundant / .nonredundant), which
// read its named graphs. Nextflow runs this (slow, reasoning-heavy) process in
// parallel with the other datasources' ingests.

process build_ubergraph {
    cache "lenient"
    memory { (ontology_set.memory ? MemoryUnit.of(ontology_set.memory) : 16.GB) + 64.GB * (task.attempt-1) }
    time { 4.hour + 8.hour * (task.attempt-1) }
    errorStrategy { task.exitStatus in 137..140 ? 'retry' : 'terminate' }
    maxRetries 3

    input:
    tuple val(subgraph), val(ontology_set), val(grebi_home), val(downloads_path)

    output:
    tuple val(subgraph), val(ontology_set.id), val("${downloads_path}/${subgraph}/ubergraph/${ontology_set.id}.nq.gz")

    script:
    def set_id = ontology_set.id
    def graph_prefix = ontology_set.graph_prefix ?: "https://w3id.org/owlmake/ubergraph"
    def offline = ontology_set.offline ? "--offline" : ""
    def out_nq = "${downloads_path}/${subgraph}/ubergraph/${set_id}.nq.gz"

    // Per-ontology provenance map: each ontology's declared IRI (the
    // rdfs:isDefinedBy target owlmake stamps on its terms) -> the datasource
    // Ontologies.<id>. The Ontologies ingest passes this to grebi_rdf2jsonl so
    // each term is tagged with the ontology it came from. Written next to the .nq.gz
    // (always written, possibly {}, so the ingest's --datasource-from-isdefinedby
    // path always resolves).
    def ds_map_path = "${downloads_path}/${subgraph}/ubergraph/${set_id}.datasource_map.json"
    def ds_map = [:]
    ontology_set.ontologies.each { ont ->
        if (ont.iri) { ds_map[ont.iri.toString()] = "Ontologies.${ont.id}".toString() }
    }
    def ds_map_json = groovy.json.JsonOutput.toJson(ds_map)

    // Resolve each ontology to a local OWL file (paths/EBI mirrors first, then
    // URLs), mirroring the datasource download source-fallback logic.
    def resolve_lines = []
    def input_args = []
    ontology_set.ontologies.each { ont ->
        // Preserve the source's extension so `om` detects the OWL format
        // (.owl/.ofn/.ttl/.obo/.rdf/.omn).
        def first = ont.sources[0]
        // Derive the extension from the basename only (strip any path and query
        // string) so a source URL whose final path segment has no dot doesn't
        // produce a bogus filename with an embedded slash.
        def base = first.tokenize('/').last().tokenize('?').first()
        def ext = base.contains('.') ? base.substring(base.lastIndexOf('.') + 1) : 'owl'
        def local = "owl/${ont.id}.${ext}"
        input_args << "-i ${local}"
        resolve_lines << "resolved=0"
        ont.sources.each { src ->
            if (src.contains("://")) {
                resolve_lines << "if [ \$resolved -eq 0 ] && curl -fSL -o \"${local}\" \"${src}\"; then resolved=1; echo \"fetched ${ont.id} from ${src}\"; fi"
            } else {
                def resolved_src = src.startsWith("/") ? src : "${grebi_home}/${src}"
                resolve_lines << "if [ \$resolved -eq 0 ] && [ -e \"${resolved_src}\" ]; then ln -sf \"\$(realpath \"${resolved_src}\")\" \"${local}\"; resolved=1; echo \"linked ${ont.id} from ${resolved_src}\"; fi"
            }
        }
        resolve_lines << "if [ \$resolved -eq 0 ]; then echo \"FAILED to resolve ${ont.id}\" >&2; exit 1; fi"
    }

    """
    #!/usr/bin/env bash
    set -Eeuo pipefail
    mkdir -p owl out
    mkdir -p "\$(dirname "${out_nq}")"
    ${resolve_lines.join("\n    ")}
    echo "Building ubergraph '${set_id}' from ${ontology_set.ontologies.size()} ontolog(y/ies) ..."
    om ubergraph ${input_args.join(" ")} --graph-prefix "${graph_prefix}" ${offline} -o out
    gzip -c out/ubergraph.nq > "${out_nq}"
    cat > "${ds_map_path}" << 'DSMAP_EOF'
${ds_map_json}
DSMAP_EOF
    echo "Wrote ${out_nq} and ${ds_map_path}"
    """
}
