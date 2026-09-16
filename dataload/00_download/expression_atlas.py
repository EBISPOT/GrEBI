#!/usr/bin/env python3
"""Discover baseline gene-level Atlas files from the FTP experiments.json catalogue.

Reads a local catalogue and emits a JSON array of ordinary GrEBI download
entries. The download workflow retrieves the catalogue and the discovered files;
this selector makes no network requests and contains no study accession list.
"""

import argparse
import json
import re
import sys


def download_entries(catalogue: dict, source_roots: list[str]) -> list[dict]:
    if not isinstance(catalogue, dict) or not isinstance(catalogue.get("experiments"), list):
        raise ValueError("Expected an experiments array")
    accessions = []
    for experiment in catalogue["experiments"]:
        if not isinstance(experiment, dict) or not isinstance(experiment.get("rawExperimentType"), str):
            raise ValueError("Missing/invalid experiment type")
        if experiment["rawExperimentType"] != "RNASEQ_MRNA_BASELINE":
            continue
        accession = experiment.get("experimentAccession")
        if not isinstance(accession, str) or not re.fullmatch(r"E-[A-Z]+-\d+", accession):
            raise ValueError("Invalid Atlas accession")
        accessions.append(accession)
    if not accessions or len(accessions) != len(set(accessions)):
        raise ValueError("Expected nonempty, unique baseline RNA-seq accessions")
    if not source_roots or any(not isinstance(root, str) or not root.strip() for root in source_roots):
        raise ValueError("At least one source root is required")
    entries = []
    for accession in sorted(accessions):
        for suffix in ("-tpms.tsv", "-configuration.xml", ".condensed-sdrf.tsv"):
            relative = f"{accession}/{accession}{suffix}"
            entries.append({
                "dest": f"expression_atlas/experiments/{relative}",
                "sources": [f"{root.rstrip('/')}/{relative}" for root in source_roots],
            })
    return entries


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-root", action="append", required=True,
                        help="Experiment-directory NFS path or FTP/HTTPS URL; repeat for fallbacks")
    parser.add_argument("catalogue", type=argparse.FileType("r"))
    args = parser.parse_args()
    try:
        entries = download_entries(json.load(args.catalogue), args.source_root)
        json.dump(entries, sys.stdout, indent=2)
        sys.stdout.write("\n")
        print(f"Discovered {len(entries) // 3} baseline studies ({len(entries)} files)", file=sys.stderr)
    except (ValueError, KeyError, TypeError) as error:
        parser.exit(1, f"Invalid Atlas catalogue/config: {error}\n")
