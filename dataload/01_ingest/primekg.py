#!/usr/bin/env python3
"""Ingest PrimeKG's kg.csv (one edge per row) as GrEBI JSONL.

Each row yields the source node with the edge as a reified property, and a
bare record for the target node so it exists even if it is never a source.
Ids are the row's source name and local id as a CURIE; the prefix normaliser
maps the source names (MONDO, HPO, GO, UBERON, REACTOME, DrugBank, UMLS) onto
the graph's prefixes. Two source names are not prefixes and are rewritten:
NCBI (gene ids) to NCBIGene, and CTD, whose exposure ids are MeSH descriptor
and supplementary-record UIs (D000075182, C092102), to MESH, so the exposures
land on the MeSH nodes the CTD and MeSH datasources already share.
"""

import csv
import json
import sys

# PrimeKG stores the local id of these ontologies as a bare integer with the
# leading zeros dropped (MONDO 5083 for MONDO:0005083, UBERON 2 for
# UBERON:0000002). All four use 7-digit zero-padded local ids, and without the
# padding the node never merges with the ontology term (issue #58).
PADDED_SOURCES = {"MONDO": 7, "HPO": 7, "GO": 7, "UBERON": 7}

SOURCE_PREFIX = {"NCBI": "NCBIGene", "CTD": "MESH"}


def curie(source, local_id):
    width = PADDED_SOURCES.get(source)
    if width and local_id.isdigit():
        local_id = local_id.zfill(width)
    return SOURCE_PREFIX.get(source, source) + ":" + local_id


def records(row):
    x_id = curie(row["x_source"], row["x_id"])
    y_id = curie(row["y_source"], row["y_id"])
    yield {
        "id": x_id,
        "grebi:name": row["x_name"],
        "grebi:type": "biolink:Entity",
        "primekg:" + row["relation"]: {
            "grebi:value": y_id,
            "grebi:properties": {"primekg:" + key: [value] for key, value in row.items()},
        },
    }
    yield {"id": y_id, "grebi:name": row["y_name"], "grebi:type": "biolink:Entity"}


def main():
    out = sys.stdout
    for row in csv.DictReader(sys.stdin):
        for record in records(row):
            out.write(json.dumps(record))
            out.write("\n")


if __name__ == "__main__":
    main()
