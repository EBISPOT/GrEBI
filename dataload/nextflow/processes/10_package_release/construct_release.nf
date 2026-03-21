process construct_release {
    cache "lenient"
    memory "4 GB"
    time "8h"
    cpus "4"

    input:
    path(neo_db)
    path(solr_dir)
    path(postgres_db)
    path(sqlite)
    path(metadata_json)
    path(query_templates)
    val(subgraph)
    val(out_dir)
    val(docker_image)
    val(dataload_home)

    publishDir "${out_dir}", overwrite: true, mode: 'copy'

    output:
    path("${subgraph}")

    script:
    """
    #!/usr/bin/env bash
    set -Eeuo pipefail

    RELEASE_DIR="${subgraph}"
    mkdir -p "\$RELEASE_DIR"

    # Symlink database artefacts into the release directory
    ln -s \$(readlink -f ${neo_db}) "\$RELEASE_DIR/"
    ln -s \$(readlink -f ${solr_dir}) "\$RELEASE_DIR/"
    ln -s \$(readlink -f ${postgres_db}) "\$RELEASE_DIR/"
    ln -s \$(readlink -f ${sqlite}) "\$RELEASE_DIR/"
    cp ${metadata_json} "\$RELEASE_DIR/"
    cp -r ${query_templates} "\$RELEASE_DIR/query_templates"

    # Generate the run script
    python3 ${dataload_home}/scripts/generate_run_script.py \
        --subgraph ${subgraph} \
        --image ${docker_image} \
        -o "\$RELEASE_DIR/grebi.sh"
    """
}
