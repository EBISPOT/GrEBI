#!/usr/bin/env python3
"""Generate a bash script that runs the GrEBI combined Docker image.

The script is meant to be bundled into the release tarball so that users can
simply extract the tarball and run ``./run_grebi.sh`` to get a fully working
GrEBI stack.

Usage:
    python3 generate_run_script.py --subgraph <name> --image <docker_image> -o run_grebi.sh
"""

import argparse
import os
import stat
import textwrap


def generate_run_script(subgraph: str, image: str) -> str:
    return textwrap.dedent(f"""\
        #!/usr/bin/env bash
        set -euo pipefail

        # -------------------------------------------------------------------
        # run_grebi.sh — start the GrEBI stack for the "{subgraph}" subgraph
        #
        # Extract the release tarball and run this script from the extracted
        # directory.  All databases are expected to be in the same directory
        # as this script.
        #
        # Requirements: Docker or Singularity/Apptainer
        #
        # Usage:
        #   ./run_grebi.sh              # start the stack (default)
        #   ./run_grebi.sh run          # same as above
        #   ./run_grebi.sh bash         # open an interactive shell
        #
        # Override container runtime:
        #   GREBI_RUNTIME=singularity ./run_grebi.sh
        #   GREBI_RUNTIME=docker ./run_grebi.sh
        #
        # Ports exposed on the host (Docker only — Singularity uses host networking):
        #   8080  — GrEBI UI
        #   8090  — GrEBI API
        #   7474  — Neo4j Browser
        #   7687  — Neo4j Bolt
        #   8983  — Solr Admin
        #   5432  — PostgreSQL
        # -------------------------------------------------------------------

        IMAGE="{image}"
        SUBGRAPH="{subgraph}"
        MODE="${{1:-run}}"
        SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
        DATA_DIR="${{GREBI_DATA_DIR:-$SCRIPT_DIR}}"

        # Detect container runtime (prefer Docker)
        detect_runtime() {{
            if [ -n "${{GREBI_RUNTIME:-}}" ]; then
                echo "$GREBI_RUNTIME"
                return
            fi
            if command -v docker &>/dev/null; then
                echo "docker"
            elif command -v singularity &>/dev/null; then
                echo "singularity"
            elif command -v apptainer &>/dev/null; then
                echo "apptainer"
            else
                echo ""
            fi
        }}
        RUNTIME=$(detect_runtime)

        if [ -z "$RUNTIME" ]; then
            echo "ERROR: no container runtime found. Install Docker, Singularity, or Apptainer."
            exit 1
        fi
        echo "Using container runtime: $RUNTIME"

        # Validate that the required files/directories exist
        missing=0
        for d in "${{SUBGRAPH}}_neo4j" "solr" "postgres_data_${{SUBGRAPH}}"; do
            if [ ! -d "$DATA_DIR/$d" ]; then
                echo "ERROR: missing directory $DATA_DIR/$d"
                missing=1
            fi
        done
        for f in "${{SUBGRAPH}}.sqlite3" "${{SUBGRAPH}}_metadata.json"; do
            if [ ! -f "$DATA_DIR/$f" ]; then
                echo "ERROR: missing $DATA_DIR/$f"
                missing=1
            fi
        done
        if [ ! -d "$DATA_DIR/query_templates" ]; then
            echo "ERROR: missing $DATA_DIR/query_templates/"
            missing=1
        fi
        if [ "$missing" -ne 0 ]; then
            echo ""
            echo "Make sure you are running this script from inside the extracted release directory."
            exit 1
        fi

        echo "Starting GrEBI ($SUBGRAPH) ..."
        echo ""

        # Stop any existing GrEBI containers using the same image
        if [ "$RUNTIME" = "docker" ]; then
            OLD=$(docker ps -q --filter "ancestor=$IMAGE" 2>/dev/null)
            if [ -n "$OLD" ]; then
                echo "Stopping previous GrEBI container(s)..."
                docker stop $OLD >/dev/null 2>&1 || true
                sleep 1
            fi
        fi

        echo "  UI:             http://localhost:8080"
        echo "  API:            http://localhost:8090"
        echo "  Neo4j Browser:  http://localhost:7474"
        echo "  Neo4j Bolt:     bolt://localhost:7687"
        echo "  Solr Admin:     http://localhost:8983"
        echo "  PostgreSQL:     localhost:5432"
        echo ""

        ENV_VARS=(
            GREBI_METADATA_JSON_SEARCH_PATH=/data
            GREBI_SQLITE_SEARCH_PATH=/data
            GREBI_QUERY_TEMPLATES_PATH=/data/query_templates
            PUBLIC_URL=/
        )

        if [ "$RUNTIME" = "docker" ]; then
            docker run --rm -it \\
                -u "$(id -u):$(id -g)" \\
                -v "$DATA_DIR:/data" \\
                -p 8080:8080 \\
                -p 8090:8090 \\
                -p 7474:7474 \\
                -p 7687:7687 \\
                -p 8983:8983 \\
                -p 5432:5432 \\
                $(printf -- '-e %s ' "${{ENV_VARS[@]}}") \\
                -w /data \\
                "$IMAGE" "$MODE"
        else
            # Singularity / Apptainer
            SIF="${{GREBI_SIF:-docker://$IMAGE}}"

            export "${{ENV_VARS[@]}}"

            if [ "$MODE" = "bash" ]; then
                "$RUNTIME" shell \\
                    --bind "$DATA_DIR:/data" \\
                    --writable-tmpfs \\
                    --pwd /data \\
                    "$SIF"
            else
                "$RUNTIME" run \\
                    --bind "$DATA_DIR:/data" \\
                    --writable-tmpfs \\
                    --pwd /data \\
                    "$SIF"
            fi
        fi
    """)


def main():
    parser = argparse.ArgumentParser(description="Generate a GrEBI run script")
    parser.add_argument("--subgraph", required=True, help="Subgraph name")
    parser.add_argument(
        "--image",
        default="ghcr.io/ebispot/grebi_combined:dev",
        help="Docker image to use (default: ghcr.io/ebispot/grebi_combined:dev)",
    )
    parser.add_argument("-o", "--output", required=True, help="Output script path")
    args = parser.parse_args()

    script = generate_run_script(args.subgraph, args.image)

    with open(args.output, "w") as f:
        f.write(script)

    os.chmod(args.output, os.stat(args.output).st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)

    print(f"Generated {args.output}")


if __name__ == "__main__":
    main()
