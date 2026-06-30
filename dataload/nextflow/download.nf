
nextflow.enable.dsl=2

import groovy.json.JsonSlurper
import groovy.yaml.YamlSlurper

params.subgraph = "$GREBI_SUBGRAPH"
params.grebi_home = "$GREBI_HOME"
params.downloads_path = "$GREBI_DOWNLOADS_PATH"

process download_file {
    cache "lenient"
    memory 2.GB
    time { 2.hour + 4.hour * (task.attempt-1) }
    errorStrategy { task.exitStatus in 137..140 ? 'retry' : 'terminate' }
    maxRetries 3

    input:
    val(download_entry)

    output:
    val(download_entry.dest)

    script:
    def dest = download_entry.dest
    def sources = download_entry.sources
    def optional = download_entry.optional ?: false
    def downloads_path = params.downloads_path
    def grebi_home = params.grebi_home

    def script_lines = []
    script_lines << "#!/usr/bin/env bash"
    script_lines << "set -Eeuo pipefail"
    script_lines << ""

    // Build a script that tries each source in order (paths first, then URLs)
    for (int i = 0; i < sources.size(); i++) {
        def source = sources[i]
        def is_url = source.contains("://")
        def is_last = (i == sources.size() - 1)
        def is_tarball = source.endsWith(".tar.gz") || source.endsWith(".tgz")
        def is_zip = source.endsWith(".zip")
        def dest_is_dir = dest.endsWith("/")

        if (is_url) {
            if (is_tarball && dest_is_dir) {
                // Download tarball and extract into dest directory
                script_lines << "# Source ${i+1}: URL (tarball extract)"
                script_lines << "echo \"Trying source: ${source}\""
                script_lines << "mkdir -p \"${downloads_path}/${dest}\""
                script_lines << "if curl -fSL --retry 5 --retry-all-errors --retry-delay 10 \"${source}\" | tar xzf - -C \"${downloads_path}/${dest}\"; then"
                script_lines << "    echo \"Success: downloaded and extracted ${source}\""
                script_lines << "    exit 0"
                script_lines << "else"
                if (is_last) {
                    if (optional) {
                        script_lines << "    echo \"OPTIONAL: all sources missing for dest ${dest} — skipping (marked optional)\""
                        script_lines << "    exit 0"
                    } else {
                        script_lines << "    echo \"FAILED: all sources exhausted for dest ${dest}\""
                        script_lines << "    exit 1"
                    }
                } else {
                    script_lines << "    echo \"Failed, trying next source...\""
                }
                script_lines << "fi"
            } else if (is_zip && dest_is_dir) {
                // Download zip and extract into dest directory
                script_lines << "# Source ${i+1}: URL (zip extract)"
                script_lines << "echo \"Trying source: ${source}\""
                script_lines << "mkdir -p \"${downloads_path}/${dest}\""
                script_lines << "GREBI_TMPZIP=\"${downloads_path}/${dest}/.grebi_tmp_download.zip\""
                script_lines << "if curl -fSL --retry 5 --retry-all-errors --retry-delay 10 -C - -o \"\$GREBI_TMPZIP\" \"${source}\" && unzip -o \"\$GREBI_TMPZIP\" -d \"${downloads_path}/${dest}\"; then"
                script_lines << "    rm -f \"\$GREBI_TMPZIP\""
                script_lines << "    echo \"Success: downloaded and extracted ${source}\""
                script_lines << "    exit 0"
                script_lines << "else"
                script_lines << "    rm -f \"\$GREBI_TMPZIP\""
                if (is_last) {
                    if (optional) {
                        script_lines << "    echo \"OPTIONAL: all sources missing for dest ${dest} — skipping (marked optional)\""
                        script_lines << "    exit 0"
                    } else {
                        script_lines << "    echo \"FAILED: all sources exhausted for dest ${dest}\""
                        script_lines << "    exit 1"
                    }
                } else {
                    script_lines << "    echo \"Failed, trying next source...\""
                }
                script_lines << "fi"
            } else if (dest_is_dir) {
                // Download single file into directory (derive filename from URL)
                def filename = source.tokenize('/').last().split('\\?')[0]
                script_lines << "# Source ${i+1}: URL (single file into dir)"
                script_lines << "echo \"Trying source: ${source}\""
                script_lines << "mkdir -p \"${downloads_path}/${dest}\""
                script_lines << "if curl -fSL --retry 5 --retry-all-errors --retry-delay 10 -C - -o \"${downloads_path}/${dest}${filename}\" \"${source}\"; then"
                script_lines << "    echo \"Success: downloaded ${source}\""
                script_lines << "    exit 0"
                script_lines << "else"
                if (is_last) {
                    if (optional) {
                        script_lines << "    echo \"OPTIONAL: all sources missing for dest ${dest} — skipping (marked optional)\""
                        script_lines << "    exit 0"
                    } else {
                        script_lines << "    echo \"FAILED: all sources exhausted for dest ${dest}\""
                        script_lines << "    exit 1"
                    }
                } else {
                    script_lines << "    echo \"Failed, trying next source...\""
                }
                script_lines << "fi"
            } else {
                // Download single file
                script_lines << "# Source ${i+1}: URL"
                script_lines << "echo \"Trying source: ${source}\""
                script_lines << "mkdir -p \"\$(dirname \"${downloads_path}/${dest}\")\""
                script_lines << "if curl -fSL --retry 5 --retry-all-errors --retry-delay 10 -C - -o \"${downloads_path}/${dest}\" \"${source}\"; then"
                script_lines << "    echo \"Success: downloaded ${source}\""
                script_lines << "    exit 0"
                script_lines << "else"
                if (is_last) {
                    if (optional) {
                        script_lines << "    echo \"OPTIONAL: all sources missing for dest ${dest} — skipping (marked optional)\""
                        script_lines << "    exit 0"
                    } else {
                        script_lines << "    echo \"FAILED: all sources exhausted for dest ${dest}\""
                        script_lines << "    exit 1"
                    }
                } else {
                    script_lines << "    echo \"Failed, trying next source...\""
                }
                script_lines << "fi"
            }
        } else {
            // File path source — resolve relative to GREBI_HOME if not absolute
            def resolved_source = source.startsWith("/") ? source : "${grebi_home}/${source}"

            if (dest_is_dir) {
                // Glob source into directory — symlink all matching files
                script_lines << "# Source ${i+1}: path (glob into dir)"
                script_lines << "echo \"Trying source: ${resolved_source}\""
                script_lines << "shopt -s nullglob"
                script_lines << "files=(${resolved_source})"
                script_lines << "shopt -u nullglob"
                script_lines << "if [ \${#files[@]} -gt 0 ]; then"
                script_lines << "    mkdir -p \"${downloads_path}/${dest}\""
                script_lines << "    for f in \"\${files[@]}\"; do"
                script_lines << "        ln -sf \"\$(realpath \"\$f\")\" \"${downloads_path}/${dest}/\$(basename \"\$f\")\""
                script_lines << "    done"
                script_lines << "    echo \"Success: symlinked \${#files[@]} files from ${resolved_source}\""
                script_lines << "    exit 0"
                script_lines << "else"
                if (is_last) {
                    if (optional) {
                        script_lines << "    echo \"OPTIONAL: all sources missing for dest ${dest} — skipping (marked optional)\""
                        script_lines << "    exit 0"
                    } else {
                        script_lines << "    echo \"FAILED: all sources exhausted for dest ${dest}\""
                        script_lines << "    exit 1"
                    }
                } else {
                    script_lines << "    echo \"No files matched, trying next source...\""
                }
                script_lines << "fi"
            } else {
                // Single file path — symlink
                script_lines << "# Source ${i+1}: path (single file)"
                script_lines << "echo \"Trying source: ${resolved_source}\""
                script_lines << "if [ -e \"${resolved_source}\" ]; then"
                script_lines << "    mkdir -p \"\$(dirname \"${downloads_path}/${dest}\")\""
                script_lines << "    ln -sf \"\$(realpath \"${resolved_source}\")\" \"${downloads_path}/${dest}\""
                script_lines << "    echo \"Success: symlinked ${resolved_source}\""
                script_lines << "    exit 0"
                script_lines << "else"
                if (is_last) {
                    if (optional) {
                        script_lines << "    echo \"OPTIONAL: all sources missing for dest ${dest} — skipping (marked optional)\""
                        script_lines << "    exit 0"
                    } else {
                        script_lines << "    echo \"FAILED: all sources exhausted for dest ${dest}\""
                        script_lines << "    exit 1"
                    }
                } else {
                    script_lines << "    echo \"Not found, trying next source...\""
                }
                script_lines << "fi"
            }
        }
        script_lines << ""
    }

    script_lines.join("\n")
}

workflow {

    // Load subgraph configuration
    config = (new JsonSlurper().parse(new File(params.grebi_home, 'configs/subgraph_configs/' + params.subgraph + '.json')))

    // Load datasource configurations
    datasources = config.datasource_configs.collect { ds -> new YamlSlurper().parse(new File(params.grebi_home, ds)) }

    // Collect all download entries from all datasources, grouped by dest
    // Each download entry has: dest (string), sources (list of strings)
    // Multiple datasources may reference the same dest — merge sources, paths first then URLs
    def all_downloads = [:]  // dest -> [path_sources..., url_sources...]

    datasources.each { ds ->
        if (ds.download) {
            ds.download.each { dl ->
                def dest = dl.dest
                if (!all_downloads.containsKey(dest)) {
                    all_downloads[dest] = [paths: [] as Set, urls: [] as Set, optional: true]
                }
                // A dest is optional only if every download entry contributing to
                // it is marked optional (so a required source can't be skipped).
                if (!dl.optional) {
                    all_downloads[dest].optional = false
                }
                dl.sources.each { source ->
                    if (source.contains("://")) {
                        all_downloads[dest].urls << source
                    } else {
                        all_downloads[dest].paths << source
                    }
                }
            }
        }
    }

    // Build channel: paths first, then URLs for each dest
    def download_entries = all_downloads.collect { dest, v ->
        [dest: dest, sources: (v.paths.toList() + v.urls.toList()), optional: v.optional]
    }

    download_channel = Channel.from(download_entries)

    download_file(download_channel)
}
