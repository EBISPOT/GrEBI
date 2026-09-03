process construct_release {
    cache "lenient"
    memory "4 GB"
    time "8h"
    cpus "4"

    input:
    path(neo_dbs)
    path(postgres_db)
    path(metadata_jsons)
    path(query_templates)
    val(subgraphs)
    val(out_dir)
    val(docker_image)
    val(dataload_home)

    // Symlinks, not a copy: nothing downstream reads out/release (staging and
    // the FTP push both consume the tarballs), and a copy dereferences the
    // store symlinks below into ~4 TB of duplicate data per run.
    publishDir "${out_dir}", overwrite: true, mode: 'symlink'

    output:
    path("release")

    script:
    """
    #!/usr/bin/env bash
    set -Eeuo pipefail

    RELEASE_DIR="release"
    mkdir -p "\$RELEASE_DIR"

    # Symlink all database artefacts into the release directory
    for neo in ${neo_dbs}; do
        ln -s \$(readlink -f "\$neo") "\$RELEASE_DIR/"
    done
    ln -s \$(readlink -f ${postgres_db}) "\$RELEASE_DIR/"
    for meta in ${metadata_jsons}; do
        cp "\$meta" "\$RELEASE_DIR/"
    done
    cp -r ${query_templates} "\$RELEASE_DIR/query_templates"

    # Generate the run script
    python3 ${dataload_home}/scripts/generate_run_script.py \
        --subgraphs ${subgraphs} \
        --image ${docker_image} \
        -o "\$RELEASE_DIR/grebi_dev.sh"
    """
}
