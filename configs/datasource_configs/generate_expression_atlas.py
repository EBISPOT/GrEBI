#!/usr/bin/env python3
"""Generate explicit NFS/FTP download entries from an Atlas FTP catalogue.

The catalogue is a local copy of
https://ftp.ebi.ac.uk/pub/databases/microarray/data/atlas/experiments.json.
Outputs YAML to stdout. This generator is not run during ingestion.
"""

import argparse
import json
import math
import re
import sys


def render(catalogue: dict, threshold: float) -> str:
    if not math.isfinite(threshold) or threshold < 0:
        raise ValueError("Minimum median TPM must be finite and non-negative")
    accessions = [e["experimentAccession"] for e in catalogue["experiments"]
                  if e["rawExperimentType"] == "RNASEQ_MRNA_BASELINE"]
    if not accessions or len(accessions) != len(set(accessions)):
        raise ValueError("Expected nonempty, unique baseline RNA-seq accessions")
    if any(not re.fullmatch(r"E-[A-Z]+-\d+", a) for a in accessions):
        raise ValueError("Invalid Atlas accession")
    lines = [
        "# Generated download manifest; edit the threshold in command below.",
        "# Refresh with generate_expression_atlas.py and the FTP experiments.json catalogue.",
        f"# {len(accessions)} bulk baseline RNA-seq studies; no raw or single-cell files.",
        "id: ExpressionAtlas", "enabled: true",
        'description: "Baseline gene expression in mapped anatomical locations across species"',
        "ingests:", '  - globs: ["expression_atlas/experiments/*/*-tpms.tsv"]',
        '    memory: "4 GB"', "    command: >-",
        '      python3 "$GREBI_DATALOAD_HOME/01_ingest/expression_atlas.py"',
        f"      --min-median-tpm {threshold}", '      -- "$GREBI_INGEST_FILENAME"',
        "download:",
    ]
    for accession in sorted(accessions):
        for suffix in ("-tpms.tsv", "-configuration.xml", ".condensed-sdrf.tsv"):
            path = f"experiments/{accession}/{accession}{suffix}"
            lines.extend([
                f"  - dest: expression_atlas/{path}", "    sources:",
                f"      - /nfs/ftp/public/databases/microarray/data/atlas/{path}",
                f"      - https://ftp.ebi.ac.uk/pub/databases/microarray/data/atlas/{path}",
            ])
    return "\n".join(lines) + "\n"


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--min-median-tpm", required=True, type=float)
    parser.add_argument("catalogue", type=argparse.FileType("r"))
    args = parser.parse_args()
    try:
        sys.stdout.write(render(json.load(args.catalogue), args.min_median_tpm))
    except (ValueError, KeyError, TypeError) as error:
        parser.exit(1, f"Invalid Atlas catalogue/config: {error}\n")
