#!/usr/bin/env python3
"""Convert the PRIDE v3 projects/all JSON array to metadata-only GrEBI JSONL.

The download stage saves the bulk export; this parser reads only that local
file (or stdin). It never fetches linked files or individual API records.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections.abc import Iterator
from typing import Any, TextIO


PROJECT_ACCESSION = re.compile(r"(?:PXD|PRD|PAD)\d+")
CURIE = re.compile(r"[A-Za-z][A-Za-z0-9_.-]*:\S+")
DOI = re.compile(r"10\.\d{4,9}/\S+", re.IGNORECASE)
BIOSAMPLE = re.compile(r"SAM(?:EA|N|D)\d+", re.IGNORECASE)
CV_FIELDS = {
    "organisms": "pride:organism",
    "organismParts": "pride:organismPart",
    "diseases": "pride:disease",
    "instruments": "pride:instrument",
    "softwares": "pride:software",
    "experimentTypes": "pride:experimentType",
    "quantificationMethods": "pride:quantificationMethod",
    "identifiedPTMStrings": "pride:modification",
}
SAMPLE_FIELDS = {
    "organism": "pride:organism",
    "organism part": "pride:organismPart",
    "disease": "pride:disease",
    "cell type": "pride:cellType",
    "cell line": "pride:cellLine",
    "biosample accession number": "pride:sample",
    "biosample": "pride:sample",
    "biosamples": "pride:sample",
}


def iter_projects(stream: TextIO, chunk_size: int = 65536) -> Iterator[dict[str, Any]]:
    """Read one project at a time, enforcing a complete top-level JSON array."""
    decoder = json.JSONDecoder()
    buffer = ""
    position = 0
    eof = False

    def refill() -> None:
        nonlocal buffer, position, eof
        chunk = stream.read(chunk_size)
        buffer = buffer[position:] + chunk
        position = 0
        eof = not chunk

    def peek() -> str:
        nonlocal position
        while True:
            while position < len(buffer) and buffer[position] in " \t\r\n":
                position += 1
            if position < len(buffer):
                return buffer[position]
            if eof:
                return ""
            refill()

    if peek() != "[":
        raise ValueError("Expected a PRIDE projects JSON array")
    position += 1
    if peek() != "]":
        while True:
            if peek() != "{":
                raise ValueError("Expected a project object in PRIDE export")
            while True:
                try:
                    project, end = decoder.raw_decode(buffer, position)
                    position = end
                    break
                except json.JSONDecodeError as error:
                    if eof:
                        raise ValueError("Invalid or truncated PRIDE project JSON") from error
                    refill()
            yield project
            delimiter = peek()
            if delimiter == "]":
                break
            if delimiter != ",":
                raise ValueError("Expected ',' or ']' after a PRIDE project")
            position += 1
    position += 1
    if peek():
        raise ValueError("Unexpected content after PRIDE projects array")


def text(value: Any) -> str:
    return value.strip() if isinstance(value, str) else ""


def objects(value: Any) -> list[dict[str, Any]]:
    if isinstance(value, dict):
        return [value]
    if isinstance(value, list):
        return [item for item in value if isinstance(item, dict)]
    return []


def unique(values: list[Any]) -> list[Any]:
    seen: set[str] = set()
    result = []
    for value in values:
        key = json.dumps(value, sort_keys=True, ensure_ascii=False)
        if value and key not in seen:
            seen.add(key)
            result.append(value)
    return result


def identifier(value: Any) -> str | None:
    value = text(value)
    # Only record references, never file-transfer locations.
    if value.startswith(("https://", "http://")):
        return value
    if BIOSAMPLE.fullmatch(value):
        return f"biosample:{value.upper()}"
    if not CURIE.fullmatch(value):
        return None
    prefix, local = value.split(":", 1)
    if prefix.casefold() in {"ftp", "sftp", "file", "aspera", "fasp"}:
        return None
    if prefix.casefold() in {"newt", "taxon", "taxonomy", "ncbitaxon"}:
        return f"ncbitaxon:{local}"
    return value


def doi_identifier(value: Any) -> str | None:
    value = text(value)
    for prefix in ("https://doi.org/", "http://doi.org/", "doi:"):
        if value.startswith(prefix):
            value = value[len(prefix):]
            break
    return f"doi:{value}" if DOI.fullmatch(value) else None


def cv_value(value: Any) -> Any:
    """Preserve supplied ontology IDs and labels, or a literal if no ID exists."""
    if isinstance(value, str):
        return identifier(value) or text(value)
    if not isinstance(value, dict):
        return None
    accession = identifier(value.get("accession"))
    label = text(value.get("name"))
    if accession:
        if label:
            return {"grebi:value": accession, "grebi:properties": {"rdfs:label": [label]}}
        return accession
    return identifier(value.get("value")) or text(value.get("value")) or label


def parse_project(project: dict[str, Any]) -> dict[str, Any]:
    accession = text(project.get("accession"))
    if not PROJECT_ACCESSION.fullmatch(accession):
        raise ValueError(f"Missing or invalid PRIDE project accession: {accession!r}")
    node: dict[str, Any] = {
        "id": f"pride.project:{accession}",
        "grebi:type": "pride:Project",
    }
    # PXD identifies the same dataset in PRIDE and ProteomeXchange. PRD is a
    # legacy PRIDE accession; PAD is an affinity-proteomics accession. Neither
    # should be assigned a ProteomeXchange alias.
    aliases = [f"px:{accession}"] if accession.startswith("PXD") else []
    project_doi = doi_identifier(project.get("doi"))
    if project_doi:
        aliases.append(project_doi)
    if aliases:
        node["dcterms:identifier"] = aliases

    for source, target in {
        "title": "grebi:name",
        "projectDescription": "grebi:description",
        "submissionDate": "dcterms:submitted",
        "publicationDate": "dcterms:issued",
        "license": "dcterms:license",
        "submissionType": "pride:submissionType",
        "sampleProcessingProtocol": "pride:sampleProcessingProtocol",
        "dataProcessingProtocol": "pride:dataProcessingProtocol",
    }.items():
        value = text(project.get(source))
        if value:
            node[target] = value

    for source, target in {"keywords": "pride:keyword", "projectTags": "pride:tag"}.items():
        values = unique([text(value) for value in project.get(source) or []])
        if values:
            node[target] = values

    for source, target in CV_FIELDS.items():
        values = unique([cv_value(value) for value in objects(project.get(source))])
        if values:
            node[target] = values

    for attribute in objects(project.get("sampleAttributes")):
        key = attribute.get("key") or {}
        target = SAMPLE_FIELDS.get(text(key.get("name")).casefold())
        raw_values = attribute.get("value") or []
        if not isinstance(raw_values, list):
            raw_values = [raw_values]
        values = [cv_value(value) for value in raw_values]
        if target:
            node[target] = unique(node.get(target, []) + values)

    people = objects(project.get("submitters")) + objects(project.get("labPIs"))
    creators = []
    for person in people:
        name = text(person.get("name")) or " ".join(
            part for part in (text(person.get("firstName")), text(person.get("lastName"))) if part
        )
        orcid = text(person.get("orcid")).removeprefix("https://orcid.org/")
        if re.fullmatch(r"\d{4}-\d{4}-\d{4}-\d{3}[\dX]", orcid):
            creator: Any = f"orcid:{orcid}"
            if name:
                creator = {"grebi:value": creator, "grebi:properties": {"rdfs:label": [name]}}
            creators.append(creator)
        elif name:
            creators.append(name)
    if creators:
        node["dcterms:creator"] = unique(creators)
    affiliations = unique([text(person.get("affiliation")) for person in people])
    if affiliations:
        node["pride:organisation"] = affiliations

    references = []
    for reference in objects(project.get("references")):
        pmid = str(reference.get("pubmedID") or "").strip()
        if pmid.isdecimal() and int(pmid) > 0:
            references.append(f"pubmed:{pmid}")
        doi = doi_identifier(reference.get("doi"))
        if doi:
            references.append(doi)
    references.extend(identifier(value) for value in project.get("otherOmicsLinks") or [])
    own_ids = {node["id"], *aliases}
    references = unique([value for value in references if value and value not in own_ids])
    if references:
        node["dcterms:references"] = references
    # No file inventories, download counts, or contact email addresses are
    # copied: only the explicit metadata fields above enter the graph.
    return node


def ingest(stream: TextIO, output: TextIO) -> int:
    seen: set[str] = set()
    for project in iter_projects(stream):
        node = parse_project(project)
        if node["id"] in seen:
            raise ValueError(f"Duplicate PRIDE project in export: {node['id']}")
        seen.add(node["id"])
        output.write(json.dumps(node, ensure_ascii=False, separators=(",", ":")) + "\n")
    if not seen:
        raise ValueError("PRIDE export contains no projects")
    return len(seen)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input", nargs="?", type=argparse.FileType("r", encoding="utf-8"), default=sys.stdin)
    args = parser.parse_args()
    count = ingest(args.input, sys.stdout)
    print(f"Ingested {count} PRIDE projects", file=sys.stderr)


if __name__ == "__main__":
    main()
