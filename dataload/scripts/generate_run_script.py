#!/usr/bin/env python3
"""Generate a bash script that runs the GrEBI combined Docker image.

The script is meant to be bundled into the release tarball so that users can
simply extract the tarball and run ``./grebi.sh`` to get a fully working
GrEBI stack.

Usage:
    python3 generate_run_script.py --subgraph <name> --image <docker_image> -o grebi.sh
"""

import argparse
import os
import stat
import textwrap


def generate_run_script(subgraphs: str, image: str) -> str:
    return textwrap.dedent(f"""\
        #!/usr/bin/env bash
        set -euo pipefail

        # -------------------------------------------------------------------
        # grebi.sh — start the GrEBI stack for subgraphs: {subgraphs}
        #
        # Extract the release tarball and run this script from the extracted
        # directory.  All databases are expected to be in the same directory
        # as this script.
        #
        # Requirements: Docker or Singularity/Apptainer
        #
        # Usage:
        #   ./grebi.sh                              # start all services
        #   ./grebi.sh api postgres solr             # start only named services
        #   ./grebi.sh -api -ui                      # start all except api and ui
        #   ./grebi.sh api -ui                       # start only api (-ui ignored)
        #   ./grebi.sh bash                          # open an interactive shell
        #
        # Prefix a service name with - to exclude it.  Exclusions only take
        # effect when no positive (include) services are given.
        #
        # Valid service names:
        #   api  cypher_service  neo4j  metadata_service  postgres
        #   prefix_service  resolver_service  solr  ui
        #
        # By default neo4j runs as a standalone server and cypher_service
        # connects to it via bolt.  If you explicitly list cypher_service
        # without neo4j, it uses embedded mode instead.
        #
        # Override container runtime:
        #   GREBI_RUNTIME=singularity ./grebi.sh
        #   GREBI_RUNTIME=docker ./grebi.sh
        #
        # Ports exposed on the host (Docker only — Singularity uses host networking):
        #   7474  — Neo4j Browser
        #   7687  — Neo4j Bolt
        #   8080  — GrEBI UI
        #   8085  — Cypher Service
        #   8090  — GrEBI API
        #   8983  — Solr Admin
        #   5432  — PostgreSQL
        # -------------------------------------------------------------------

        IMAGE="{image}"
        SUBGRAPHS="{subgraphs}"
        SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
        DATA_DIR="${{GREBI_DATA_DIR:-$SCRIPT_DIR}}"

        ALL_SERVICES="api cypher_service neo4j metadata_service postgres prefix_service resolver_service solr ui"

        # Parse arguments: "bash" and "test" are modes; anything else is a service name.
        # A leading dash (e.g. -api) marks a service for exclusion.
        MODE="run"
        INCLUDES=()
        EXCLUDES=()
        for arg in "$@"; do
            case "$arg" in
                bash|test) MODE="$arg" ;;
                -*)        EXCLUDES+=("${{arg#-}}") ;;
                *)         INCLUDES+=("$arg") ;;
            esac
        done

        # Validate service names (both includes and excludes)
        for svc in "${{INCLUDES[@]+${{INCLUDES[@]}}}}" "${{EXCLUDES[@]+${{EXCLUDES[@]}}}}"; do
            [ -z "$svc" ] && continue
            valid=0
            for ok in $ALL_SERVICES; do
                if [ "$svc" = "$ok" ]; then valid=1; break; fi
            done
            if [ "$valid" -eq 0 ]; then
                echo "ERROR: unknown service '$svc'"
                echo "Valid services: $ALL_SERVICES"
                exit 1
            fi
        done

        # Resolve the final service list:
        #   - If any includes given: use only those (excludes are ignored)
        #   - If only excludes given: start all services minus the excluded
        #   - If neither: start all services
        SERVICES=()
        if [ ${{#INCLUDES[@]}} -gt 0 ]; then
            SERVICES=("${{INCLUDES[@]}}")
        elif [ ${{#EXCLUDES[@]}} -gt 0 ]; then
            for svc in $ALL_SERVICES; do
                excluded=0
                for ex in "${{EXCLUDES[@]+${{EXCLUDES[@]}}}}"; do
                    if [ "$svc" = "$ex" ]; then excluded=1; break; fi
                done
                if [ "$excluded" -eq 0 ]; then
                    SERVICES+=("$svc")
                fi
            done
        fi

        # Build the GREBI_SERVICES env var (empty = all)
        if [ ${{#SERVICES[@]}} -gt 0 ]; then
            GREBI_SERVICES_VAL=$(IFS=,; echo "${{SERVICES[*]}}")
        else
            GREBI_SERVICES_VAL=""
        fi

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
        for d in "solr" "postgres_data"; do
            if [ ! -d "$DATA_DIR/$d" ]; then
                echo "ERROR: missing directory $DATA_DIR/$d"
                missing=1
            fi
        done
        IFS=',' read -ra SG_ARRAY <<< "$SUBGRAPHS"
        for sg in "${{SG_ARRAY[@]}}"; do
            for d in "${{sg}}_neo4j"; do
                if [ ! -d "$DATA_DIR/$d" ]; then
                    echo "ERROR: missing directory $DATA_DIR/$d"
                    missing=1
                fi
            done
            for f in "${{sg}}.sqlite3" "${{sg}}_metadata.json"; do
                if [ ! -f "$DATA_DIR/$f" ]; then
                    echo "ERROR: missing $DATA_DIR/$f"
                    missing=1
                fi
            done
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

        echo "Starting GrEBI ($SUBGRAPHS) ..."
        if [ -n "$GREBI_SERVICES_VAL" ]; then
            echo "Services: $GREBI_SERVICES_VAL"
        else
            echo "Services: all"
        fi
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

        # Helper to check if a service is enabled
        svc_enabled() {{
            if [ ${{#SERVICES[@]}} -eq 0 ]; then return 0; fi
            for s in "${{SERVICES[@]}}"; do
                if [ "$s" = "$1" ]; then return 0; fi
            done
            return 1
        }}

        echo "Endpoints:"
        svc_enabled ui               && echo "  UI:             http://localhost:8080"
        svc_enabled api              && echo "  API:            http://localhost:8090"
        svc_enabled cypher_service   && echo "  Cypher Service: http://localhost:8085"
        svc_enabled neo4j            && echo "  Neo4j Browser:  http://localhost:7474"
        svc_enabled neo4j            && echo "  Neo4j Bolt:     bolt://localhost:7687"
        svc_enabled solr             && echo "  Solr Admin:     http://localhost:8983"
        svc_enabled postgres         && echo "  PostgreSQL:     localhost:5432"
        svc_enabled metadata_service && echo "  Metadata:       http://localhost:8081"
        svc_enabled prefix_service   && echo "  Prefix:         http://localhost:8082"
        svc_enabled resolver_service && echo "  Resolver:       http://localhost:8084"
        echo ""

        ENV_VARS=(
            GREBI_METADATA_JSON_SEARCH_PATH=/data
            GREBI_SQLITE_SEARCH_PATH=/data
            GREBI_QUERY_TEMPLATES_PATH=/data/query_templates
            PUBLIC_URL=/
        )
        if [ -n "$GREBI_SERVICES_VAL" ]; then
            ENV_VARS+=("GREBI_SERVICES=$GREBI_SERVICES_VAL")
        fi
        if [ -n "${{GREBI_EXPORT_SNAPSHOTS:-}}" ]; then
            ENV_VARS+=("GREBI_EXPORT_SNAPSHOTS=$GREBI_EXPORT_SNAPSHOTS")
        fi
        if [ -n "${{GREBI_EXPECTED_DIR:-}}" ]; then
            ENV_VARS+=("GREBI_EXPECTED_DIR=$GREBI_EXPECTED_DIR")
        fi

        if [ "$RUNTIME" = "docker" ]; then
            docker run --rm -it \\
                -u "$(id -u):$(id -g)" \\
                -v "$DATA_DIR:/data" \\
                -p 7474:7474 \\
                -p 7687:7687 \\
                -p 8080:8080 \\
                -p 8085:8085 \\
                -p 8081:8081 \\
                -p 8082:8082 \\
                -p 8084:8084 \\
                -p 8090:8090 \\
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
    parser.add_argument("--subgraphs", required=True, help="Comma-separated subgraph names")
    parser.add_argument(
        "--image",
        default="ghcr.io/ebispot/grebi_combined:dev",
        help="Docker image to use (default: ghcr.io/ebispot/grebi_combined:dev)",
    )
    parser.add_argument("-o", "--output", required=True, help="Output script path")
    args = parser.parse_args()

    script = generate_run_script(args.subgraphs, args.image)

    with open(args.output, "w") as f:
        f.write(script)

    os.chmod(args.output, os.stat(args.output).st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)

    print(f"Generated {args.output}")


if __name__ == "__main__":
    main()
