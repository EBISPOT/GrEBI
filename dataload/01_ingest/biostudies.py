#!/usr/bin/env python3
"""Convert BioStudies PageTab JSON metadata to GrEBI JSONL.

The input may be an individual PageTab JSON file, a directory containing the
BioStudies FTP tree, or a single JSON document on stdin.  Directory traversal
only selects each submission's accession-level JSON document.  It deliberately
does not enter ``Files`` directories and excludes the Europe PMC collection.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from collections.abc import Iterable, Iterator
from pathlib import Path
from typing import Any
from urllib.parse import urlparse


EUROPE_PMC_NAMES = {"europepmc", "s-epmc"}
DOI_RE = re.compile(r"^10\.\d{4,9}/\S+$", re.IGNORECASE)
PMID_RE = re.compile(r"^(?:pmid:)?(\d+)$", re.IGNORECASE)
PMCID_RE = re.compile(r"^(?:pmc:?)?(PMC\d+)$", re.IGNORECASE)
ONTOLOGY_ID_RE = re.compile(
    r"^(?:https?://\S+|[A-Za-z][A-Za-z0-9_.-]*[:_][A-Za-z0-9][A-Za-z0-9_.:-]*)$"
)


def _as_objects(value: Any) -> Iterator[dict[str, Any]]:
    """Yield dictionaries from arbitrarily nested PageTab arrays."""
    if isinstance(value, dict):
        yield value
    elif isinstance(value, list):
        for item in value:
            yield from _as_objects(item)


def _attributes(owner: dict[str, Any]) -> list[dict[str, Any]]:
    return list(_as_objects(owner.get("attributes", [])))


def _attribute_values(owner: dict[str, Any], *names: str) -> list[str]:
    wanted = {name.casefold() for name in names}
    values: list[str] = []
    for attribute in _attributes(owner):
        name = str(attribute.get("name") or "").strip().casefold()
        value = attribute.get("value")
        if name in wanted and value is not None and str(value).strip():
            values.append(str(value).strip())
    return values


def _sections(section: Any) -> Iterator[dict[str, Any]]:
    """Walk PageTab sections, but never file or link objects."""
    for current in _as_objects(section):
        yield current
        yield from _sections(current.get("subsections", []))


def _deduplicate(values: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        value = value.strip()
        if value and value not in seen:
            seen.add(value)
            result.append(value)
    return result


def _first(values: Iterable[str]) -> str | None:
    return next((value for value in values if value), None)


def _link_reference(link: dict[str, Any]) -> str | None:
    value = str(link.get("url") or "").strip()
    if not value:
        return None

    parsed_url = urlparse(value)

    link_types = _attribute_values(link, "Type")
    link_type = " ".join(link_types).casefold()

    # Submission payloads are outside GrEBI's scope.  In particular, do not
    # turn FIRE/FTP paths or PageTab file links into graph identifiers.
    if "file" in link_type or parsed_url.scheme.casefold() in {"ftp", "sftp"}:
        return None

    if "doi" in link_type or DOI_RE.fullmatch(value):
        return f"doi:{value.removeprefix('doi:')}"

    pmid_match = PMID_RE.fullmatch(value)
    if "pubmed" in link_type or "pmid" in link_type:
        return f"pubmed:{pmid_match.group(1)}" if pmid_match else None

    pmcid_match = PMCID_RE.fullmatch(value)
    if "pmc" in link_type and pmcid_match:
        return f"pmc:{pmcid_match.group(1).removeprefix('PMC')}"

    if "array design" in link_type and value.upper().startswith("A-"):
        return f"arrayexpress.platform:{value}"

    if re.fullmatch(r"PRJ[A-Z]+\d+", value, re.IGNORECASE):
        return f"insdc:{value.upper()}"

    # Older PageTab records commonly use the legacy ENA data-view URL.  Map
    # both it and the current browser URL to the same INSDC project identifier
    # used by GrEBI's ENA datasource so that the reference becomes an edge.
    if parsed_url.hostname in {"ebi.ac.uk", "www.ebi.ac.uk"}:
        ena_match = re.fullmatch(
            r"/ena/(?:data|browser)/view/(PRJ[A-Z]+\d+)/?",
            parsed_url.path,
            re.IGNORECASE,
        )
        if ena_match:
            return f"insdc:{ena_match.group(1).upper()}"

    if value.upper().startswith("EMPIAR-"):
        return f"empiar:{value.removeprefix('EMPIAR-')}"

    if parsed_url.scheme.casefold() in {"http", "https"}:
        return value

    return None


def _links(section: Any) -> Iterator[dict[str, Any]]:
    for current in _sections(section):
        yield from _as_objects(current.get("links", []))


def _alternative_identifiers(accession: str, collections: list[str]) -> list[str]:
    result: list[str] = []
    collection_names = {value.casefold() for value in collections}

    if "arrayexpress" in collection_names and accession.upper().startswith(("A-", "E-")):
        result.append(f"arrayexpress:{accession}")
    if accession.upper().startswith("EMPIAR-"):
        result.append(f"empiar:{accession.removeprefix('EMPIAR-')}")
    if accession.upper().startswith(("BIOMD", "MODEL")):
        result.append(f"biomodels.db:{accession}")

    return result


def _is_europe_pmc(accession: str, collections: list[str]) -> bool:
    accession_lower = accession.casefold()
    return (
        accession_lower == "europepmc"
        or accession_lower.startswith("s-epmc")
        or any(value.casefold() in EUROPE_PMC_NAMES for value in collections)
    )


def parse_submission(submission: dict[str, Any]) -> dict[str, Any] | None:
    """Return a metadata-only GrEBI node for one BioStudies submission."""
    accession = str(submission.get("accno") or "").strip()
    if not accession:
        raise ValueError("BioStudies PageTab document has no accno")

    collections = _deduplicate(_attribute_values(submission, "AttachTo"))
    if _is_europe_pmc(accession, collections):
        return None

    root_section = submission.get("section")
    section_objects = list(_sections(root_section))
    primary_section = section_objects[0] if section_objects else {}

    title = _first(
        _attribute_values(submission, "Title")
        + _attribute_values(primary_section, "Title")
    )
    descriptions = _deduplicate(_attribute_values(primary_section, "Description"))
    release_date = _first(_attribute_values(submission, "ReleaseDate", "Release Date"))

    section_type = str(primary_section.get("type") or "").strip().casefold()
    node_type = (
        "biostudies:Collection"
        if section_type in {"collection", "project"}
        else "biostudies:Study"
    )

    node: dict[str, Any] = {
        "id": f"biostudies:{accession}",
        "grebi:type": node_type,
    }
    if title:
        node["grebi:name"] = title
    if descriptions:
        node["grebi:description"] = descriptions[0] if len(descriptions) == 1 else descriptions
    if release_date:
        node["dcterms:issued"] = release_date
    if collections:
        node["biostudies:collection"] = [f"biostudies:{value}" for value in collections]

    alternative_ids = _alternative_identifiers(accession, collections)
    if alternative_ids:
        node["dcterms:identifier"] = alternative_ids

    study_types: list[str] = []
    organisms: list[str] = []
    authors: list[str] = []
    organisations: list[str] = []
    ontology_terms: list[str] = []
    references: list[str] = []

    for section in section_objects:
        study_types.extend(
            _attribute_values(
                section,
                "Study type",
                "Study types",
                "Experimental Design",
                "Experimental Designs",
            )
        )
        organisms.extend(_attribute_values(section, "Organism"))

        current_type = str(section.get("type") or "").strip().casefold()
        if current_type == "author":
            authors.extend(_attribute_values(section, "Name"))
        elif current_type in {"organization", "organisation"}:
            organisations.extend(_attribute_values(section, "Name"))
        elif current_type == "publication":
            publication_accession = str(section.get("accno") or "").strip()
            pmid_match = PMID_RE.fullmatch(publication_accession)
            if pmid_match:
                references.append(f"pubmed:{pmid_match.group(1)}")
            for doi in _attribute_values(section, "DOI"):
                if DOI_RE.fullmatch(doi):
                    references.append(f"doi:{doi}")

        for attribute in _attributes(section):
            for qualifier in _as_objects(attribute.get("valqual", [])):
                if str(qualifier.get("name") or "").strip().casefold() != "termid":
                    continue
                term_id = str(qualifier.get("value") or "").strip()
                if ONTOLOGY_ID_RE.fullmatch(term_id):
                    ontology_terms.append(term_id)

    for link in _links(root_section):
        reference = _link_reference(link)
        if reference:
            references.append(reference)

    for key, values in (
        ("biostudies:studyType", study_types),
        ("biostudies:organism", organisms),
        ("dcterms:creator", authors),
        ("biostudies:organisation", organisations),
        ("biostudies:ontologyTerm", ontology_terms),
        ("dcterms:references", references),
    ):
        unique_values = _deduplicate(values)
        if unique_values:
            node[key] = unique_values

    return node


def _page_tab_files(root: Path) -> Iterator[Path]:
    """Yield accession-level JSON files from a BioStudies FTP/NFS tree."""
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = sorted(
            name
            for name in dirnames
            if name.casefold() not in EUROPE_PMC_NAMES and name.casefold() != "files"
        )
        directory = Path(dirpath)
        expected_name = f"{directory.name}.json"
        if expected_name in filenames:
            yield directory / expected_name


def _input_documents(paths: list[Path]) -> Iterator[tuple[str, dict[str, Any]]]:
    for path in paths:
        candidates = _page_tab_files(path) if path.is_dir() else iter((path,))
        for candidate in candidates:
            with candidate.open(encoding="utf-8") as stream:
                document = json.load(stream)
            if not isinstance(document, dict):
                raise ValueError(f"{candidate}: expected a JSON object")
            yield str(candidate), document


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "paths",
        nargs="*",
        type=Path,
        help="PageTab JSON files or BioStudies tree directories",
    )
    args = parser.parse_args(argv)

    if args.paths:
        documents = _input_documents(args.paths)
    else:
        document = json.load(sys.stdin)
        if not isinstance(document, dict):
            raise ValueError("stdin: expected a JSON object")
        documents = iter((("stdin", document),))

    seen: set[str] = set()
    for source, document in documents:
        try:
            node = parse_submission(document)
        except Exception as error:
            raise ValueError(f"{source}: {error}") from error
        if node is not None and node["id"] not in seen:
            seen.add(node["id"])
            print(json.dumps(node, ensure_ascii=False, separators=(",", ":")))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
