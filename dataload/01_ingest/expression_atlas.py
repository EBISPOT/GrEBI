#!/usr/bin/env python3
"""Offline bulk Expression Atlas TPM -> gene/anatomy expression evidence.

Reads the TPM table and its sibling configuration XML and condensed SDRF.
No downloads, differential results, proteins, transcripts or individual cells.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import re
import sys
import xml.etree.ElementTree as ET
from collections import Counter, defaultdict
from pathlib import Path
from typing import TextIO


ACCESSION = re.compile(r"E-[A-Z]+-\d+")
GENE_ID = re.compile(r"[A-Za-z0-9_][A-Za-z0-9_.-]*")
ONTOLOGY_URI = re.compile(r"https?://(?:purl\.obolibrary\.org/obo/|www\.ebi\.ac\.uk/efo/)([A-Za-z]+)_([A-Za-z0-9]+)")
# Only identifiers explicitly supplied for `organism part`; do not turn cell
# types (CL) or developmental stages into anatomical locations.
ANATOMY_PREFIXES = {"uberon", "po", "fbbt", "wbbt", "zfa", "emapa", "emap", "ma", "bto", "efo", "ncit"}
NORMAL = {"normal", "healthy", "healthy individual", "normal tissue", "disease free", "disease-free"}
UNTREATED = {"none", "no treatment", "untreated", "not treated", "not applicable", "control", "mock", "mock treatment", "vehicle control", "pbs control"}
WILD_TYPE = {"wild type", "wild-type", "wild type genotype", "wild-type genotype", "wt"}
NO_CELL_LINE = {"none", "not applicable", "not a cell line"}
NO_INFECTION = {"none", "uninfected", "not infected", "mock", "mock infected", "not applicable"}
MISSING = {"", "NA", "N/A", "NULL", "NaN"}


def ontology_id(uri: str) -> str | None:
    match = ONTOLOGY_URI.fullmatch(uri)
    return f"{match[1].lower()}:{match[2]}" if match else None


def read_annotations(stream: TextIO, accession: str) -> dict:
    samples: dict = defaultdict(lambda: defaultdict(set))
    for number, row in enumerate(csv.reader(stream, delimiter="\t"), 1):
        if not row:
            continue
        if len(row) < 6 or row[0] != accession or not row[2]:
            raise ValueError(f"Malformed condensed SDRF at line {number}")
        if row[3] not in {"characteristic", "factor"}:
            continue
        value = row[5].strip()
        uris = tuple(sorted({identifier for field in row[6:] for uri in field.split()
                             if (identifier := ontology_id(uri))}))
        samples[row[2]][row[4].strip().casefold()].add((value, uris))
    if not samples:
        raise ValueError("Empty condensed SDRF")
    return samples


def exclusion_reason(sample: dict) -> str | None:
    """Conservative metadata filter; absence is not asserted to prove health.

Atlas baseline classification supplies the unperturbed-study context. When
metadata explicitly describe disease/perturbation, only recognised controls
are admitted; missing/unknown values in these fields are not controls.
"""
    for key, annotations in sample.items():
        values = {value.casefold() for value, _ in annotations}
        if "cell line" in key and not values <= NO_CELL_LINE:
            return "cell_line"
        if key in {"disease", "disease state", "disease status"} and not values <= NORMAL:
            return "disease"
        if key == "genotype" and not values <= WILD_TYPE:
            return "genotype"
        if any(word in key for word in ("treatment", "compound", "stimulus", "perturbation", "drug")):
            if not values <= UNTREATED:
                return "treatment"
        if "infection" in key or "infected" in key:
            if not values <= NO_INFECTION:
                return "infection"
    return None


def group_metadata(configuration: ET.Element, samples: dict) -> tuple[dict, Counter]:
    if configuration.get("experimentType", "").casefold() != "rnaseq_mrna_baseline":
        raise ValueError("Only rnaseq_mrna_baseline configurations are supported")
    groups = {}
    counts: Counter = Counter()
    for group in configuration.findall("./analytics/assay_groups/assay_group"):
        identifier = group.get("id")
        if not identifier or identifier in groups:
            raise ValueError(f"Missing or duplicate assay group: {identifier!r}")
        groups[identifier] = None
        assays = [a.text.strip() for a in group.findall("assay") if a.text and a.text.strip()]
        if not assays or len(assays) != len(set(assays)):
            raise ValueError(f"Missing or duplicate assays in {identifier}")
        if any(assay not in samples for assay in assays):
            raise ValueError(f"Assay missing from condensed SDRF in group {identifier}")
        records = [samples[a] for a in assays]
        reasons = {reason for sample in records if (reason := exclusion_reason(sample))}
        if reasons:
            counts["excluded_" + sorted(reasons)[0]] += 1
            continue
        locations = [{iri for _, ids in s.get("organism part", ()) for iri in ids
                      if iri.split(":", 1)[0] in ANATOMY_PREFIXES} for s in records]
        # Do not attribute a pooled/mixed group's expression to its components.
        if any(len(ids) != 1 for ids in locations) or any(ids != locations[0] for ids in locations):
            counts["excluded_unmapped_or_mixed_anatomy"] += 1
            continue
        taxa = [{iri for _, ids in s.get("organism", ()) for iri in ids
                 if iri.startswith("ncbitaxon:")} for s in records]
        if any(len(ids) != 1 for ids in taxa) or any(ids != taxa[0] for ids in taxa):
            counts["excluded_unmapped_or_mixed_taxon"] += 1
            continue
        anatomy = next(iter(locations[0]))
        labels = sorted({value for s in records for value, ids in s["organism part"] if anatomy in ids})
        context = {}
        for key in ("developmental stage", "age", "sex", "disease", "genotype", "cultivar", "strain", "growth condition"):
            values = sorted({value for s in records for value, _ in s.get(key, ()) if value})
            if values:
                context[key] = values
        groups[identifier] = {
            "anatomy": anatomy, "labels": labels, "taxon": next(iter(taxa[0])),
            "label": group.get("label", identifier), "context": context,
        }
        counts["included_groups"] += 1
    if not groups:
        raise ValueError("Configuration contains no assay groups")
    return groups, counts


def median_tpm(value: str) -> float | None:
    if value in MISSING:
        return None
    parts = value.split(",")
    if len(parts) not in {1, 5}:
        raise ValueError(f"Expected scalar or five-number TPM summary, got {value!r}")
    numbers = [float(part) for part in parts]
    if any(not math.isfinite(n) or n < 0 for n in numbers):
        raise ValueError(f"Invalid TPM value: {value!r}")
    if numbers != sorted(numbers):
        raise ValueError(f"Unordered TPM summary: {value!r}")
    return numbers[len(numbers) // 2]


def gene_node(identifier: str, name: str, taxon: str) -> dict:
    if not GENE_ID.fullmatch(identifier):
        raise ValueError(f"Invalid gene identifier: {identifier!r}")
    node = {"id": f"ensembl:{identifier}", "grebi:type": "biolink:Gene", "biolink:in_taxon": taxon}
    if name:
        node["grebi:name"] = name
    # Atlas uses the Ensembl/Ensembl Genomes annotation IDs, including model
    # organism IDs. Preserve exact, unambiguous native aliases where known.
    for pattern, prefix in ((r"WBGene\d+", "wormbase"), (r"FBgn\d+", "flybase"),
                            (r"AT[1-5CM]G\d{5}", "tair.locus")):
        if re.fullmatch(pattern, identifier):
            node["dcterms:identifier"] = [f"{prefix}:{identifier}"]
    return node


def ingest(table: TextIO, configuration: ET.Element, annotations: TextIO,
           accession: str, threshold: float, output: TextIO) -> Counter:
    if not ACCESSION.fullmatch(accession):
        raise ValueError(f"Invalid experiment accession: {accession!r}")
    if not math.isfinite(threshold) or threshold < 0:
        raise ValueError("Minimum median TPM must be finite and non-negative")
    samples = read_annotations(annotations, accession)
    groups, counts = group_metadata(configuration, samples)
    taxa = {g["taxon"] for g in groups.values() if g}
    if len(taxa) > 1:
        raise ValueError("Multiple species in a baseline experiment")
    reader = csv.reader(table, delimiter="\t")
    header = next(reader, [])
    if len(header) < 3 or header[:2] not in (["GeneID", "Gene Name"], ["Gene ID", "Gene Name"]):
        raise ValueError("Expected a gene-level TPM table header")
    if len(header[2:]) != len(set(header[2:])) or set(header[2:]) != set(groups):
        raise ValueError("TPM columns do not match configuration assay groups")
    selected = [(index, group, groups[group]) for index, group in enumerate(header[2:], 2) if groups[group]]
    seen_genes = set()
    emitted_anatomy = set()
    study = f"https://www.ebi.ac.uk/gxa/experiments/{accession}"

    def emit(node: dict) -> None:
        output.write(json.dumps(node, ensure_ascii=False, separators=(",", ":")) + "\n")

    # Keep provenance even if every group/gene is filtered out. The common
    # ingest process requires a nonempty JSONL stream (split emits no files
    # for empty input). No expression assertions are made for excluded groups.
    emit({"id": study, "grebi:type": "biolink:Dataset", "grebi:name": f"Expression Atlas {accession}",
          "expression_atlas:experiment_accession": accession,
          "expression_atlas:min_median_tpm": threshold,
          "expression_atlas:included_assay_groups": counts["included_groups"]})

    for line_number, row in enumerate(reader, 2):
        if len(row) != len(header):
            raise ValueError(f"Wrong column count in TPM row {line_number}")
        if not GENE_ID.fullmatch(row[0]) or row[0] in seen_genes:
            raise ValueError(f"Invalid or duplicate gene in TPM row {line_number}: {row[0]!r}")
        seen_genes.add(row[0])
        counts["genes_read"] += 1
        evidence: dict = defaultdict(list)
        for index, group_id, group in selected:
            value = median_tpm(row[index])
            # Strictly above the configured cutoff, including when it is zero.
            if value is None or value <= threshold:
                continue
            evidence[group["anatomy"]].append({
                "gene": f"ensembl:{row[0]}", "anatomy": group["anatomy"], "taxon": group["taxon"],
                "study": study, "assay_group": group_id, "group_label": group["label"],
                "median_tpm": value, "context": group["context"],
            })
            counts["supporting_group_results"] += 1
        if not evidence:
            continue
        node = gene_node(row[0], row[1], next(iter(taxa)))
        node["biolink:expressed_in"] = []
        node["expression_atlas:evidence"] = []
        for anatomy, observations in sorted(evidence.items()):
            if anatomy not in emitted_anatomy:
                labels = sorted({label for _, _, g in selected if g["anatomy"] == anatomy for label in g["labels"]})
                emit({"id": anatomy, "grebi:type": "biolink:AnatomicalEntity", "grebi:name": labels})
                emitted_anatomy.add(anatomy)
            # GrEBI deduplicates complete property values, including reification
            # metadata. Study-specific edge properties would create parallel
            # edges. Keep the relation identical across studies and put paired
            # observations on the gene, explicitly keyed by anatomy and taxon.
            node["biolink:expressed_in"].append({
                "grebi:value": anatomy,
                "grebi:properties": {
                    "expression_atlas:min_median_tpm": [threshold],
                },
            })
            node["expression_atlas:evidence"].extend(
                json.dumps(o, sort_keys=True, separators=(",", ":")) for o in observations)
            counts["gene_anatomy_pairs"] += 1
        emit(node)
        counts["genes_emitted"] += 1
    if not seen_genes:
        raise ValueError("TPM table contains no genes")
    return counts


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--min-median-tpm", required=True, type=float,
                        help="Include only medians strictly above this TPM cutoff (no default)")
    parser.add_argument("filename", type=Path)
    args = parser.parse_args()
    suffix = "-tpms.tsv"
    if not args.filename.name.endswith(suffix):
        parser.error("Input must be an Atlas gene-level *-tpms.tsv file")
    accession = args.filename.name[:-len(suffix)]
    try:
        configuration = ET.parse(args.filename.with_name(accession + "-configuration.xml")).getroot()
        with args.filename.open() as table, args.filename.with_name(accession + ".condensed-sdrf.tsv").open() as sdrf:
            counts = ingest(table, configuration, sdrf, accession, args.min_median_tpm, sys.stdout)
        print(json.dumps({"experiment": accession, "min_median_tpm": args.min_median_tpm, **counts}, sort_keys=True), file=sys.stderr)
    except (OSError, ValueError, ET.ParseError) as error:
        parser.exit(1, f"Expression Atlas ingest failed: {error}\n")


if __name__ == "__main__":
    main()
