#!/usr/bin/env python3
"""Fetch BioStudies PageTab JSON for every non-Europe-PMC study from the public API.

Enumerates studies through the search API, by collection and by accession
prefix (the uncollected S-* families have no collection to query), then fetches
each study's JSON into <dest>/<ACCESSION>/<ACCESSION>.json — the layout of the
BioStudies FTP tree that grebi_ingest_biostudies walks. The API document is
byte-identical to the FTP one.

Runs as a `command` download entry: one task fetches the whole tree with bounded
concurrency. Files already present are kept, so a rerun only fetches what is
new, and studies no longer enumerated are pruned so withdrawn ones do not linger.
"""

import argparse
import concurrent.futures
import json
import os
import re
import shutil
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

USER_AGENT = "GrEBI dataload (https://github.com/EBISPOT/GrEBI)"
ACCESSION = re.compile(r"[A-Z]-[A-Z]{3,4}-?\d+")
EUROPE_PMC_PREFIX = "S-EPMC"


def http_get(url: str, attempts: int = 5) -> bytes | None:
    """GET with retries and backoff. None for a 404 (withdrawn between listing and fetch)."""
    delay = 2.0
    for attempt in range(1, attempts + 1):
        try:
            request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT, "Accept": "application/json"})
            with urllib.request.urlopen(request, timeout=120) as response:
                return response.read()
        except urllib.error.HTTPError as error:
            if error.code == 404:
                return None
            last = f"HTTP {error.code}"
            if 400 <= error.code < 500 and error.code != 429:
                raise RuntimeError(f"{url}: {last}")
        except (urllib.error.URLError, TimeoutError, OSError) as error:
            last = str(error)
        if attempt == attempts:
            raise RuntimeError(f"{url}: giving up after {attempts} attempts ({last})")
        time.sleep(delay)
        delay = min(delay * 2, 60.0)


def search(api: str, params: dict) -> dict:
    return json.loads(http_get(f"{api}/search?{urllib.parse.urlencode(params)}"))


def paginate(api: str, params: dict, page_size: int, fetch=search):
    """Every hit of a search, page by page."""
    page = 1
    seen = 0
    while True:
        result = fetch(api, {**params, "pageSize": page_size, "page": page})
        hits = result.get("hits") or []
        yield from hits
        seen += len(hits)
        total = int(result.get("totalHits") or 0)
        if not hits or seen >= total:
            return
        page += 1


def enumerate_accessions(api: str, collections: list[str], prefixes: list[str],
                         page_size: int = 100, fetch=search) -> list[str]:
    """Distinct non-Europe-PMC accessions across the collections and accession prefixes."""
    accessions: set[str] = set()
    for collection in collections:
        found = 0
        for hit in paginate(api, {"collection": collection}, page_size, fetch):
            accession = hit.get("accession", "")
            if ACCESSION.fullmatch(accession) and not accession.startswith(EUROPE_PMC_PREFIX):
                accessions.add(accession)
                found += 1
        print(f"collection {collection}: {found} studies", file=sys.stderr)
    for prefix in prefixes:
        # The query is a free-text search, so it returns a superset: keep only
        # accessions that really carry the prefix.
        found = 0
        for hit in paginate(api, {"query": f"{prefix}*"}, page_size, fetch):
            accession = hit.get("accession", "")
            if accession.startswith(prefix) and ACCESSION.fullmatch(accession) and not accession.startswith(EUROPE_PMC_PREFIX):
                accessions.add(accession)
                found += 1
        print(f"prefix {prefix}: {found} studies", file=sys.stderr)
    return sorted(accessions)


def study_path(dest: Path, accession: str) -> Path:
    return dest / accession / f"{accession}.json"


def fetch_study(api: str, dest: Path, accession: str, get=http_get) -> str:
    """Fetch one study unless present. Returns 'kept', 'fetched' or 'missing'."""
    path = study_path(dest, accession)
    if path.is_file() and path.stat().st_size > 0:
        return "kept"
    body = get(f"{api}/studies/{accession}")
    if body is None:
        return "missing"
    json.loads(body)  # a truncated or HTML response must not be written as a study
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".json.part")
    tmp.write_bytes(body)
    os.replace(tmp, path)
    return "fetched"


def prune(dest: Path, accessions: list[str]) -> int:
    """Remove study directories no longer enumerated."""
    keep = set(accessions)
    removed = 0
    for entry in dest.iterdir() if dest.is_dir() else []:
        if entry.is_dir() and entry.name not in keep and ACCESSION.fullmatch(entry.name):
            shutil.rmtree(entry)
            removed += 1
    return removed


def fetch_all(api: str, dest: Path, accessions: list[str], concurrency: int, get=http_get) -> dict[str, int]:
    counts = {"kept": 0, "fetched": 0, "missing": 0, "failed": 0}
    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as pool:
        futures = {pool.submit(fetch_study, api, dest, accession, get): accession for accession in accessions}
        for done, future in enumerate(concurrent.futures.as_completed(futures), 1):
            try:
                counts[future.result()] += 1
            except Exception as error:  # one bad study must not stop the other 100k
                counts["failed"] += 1
                print(f"WARNING: {futures[future]}: {error}", file=sys.stderr)
            if done % 5000 == 0:
                print(f"{done}/{len(accessions)} studies: {counts}", file=sys.stderr)
    return counts


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--api", default="https://www.ebi.ac.uk/biostudies/api/v1")
    parser.add_argument("--collection", action="append", default=[], help="BioStudies collection to include; repeatable")
    parser.add_argument("--accession-prefix", action="append", default=[],
                        help="Accession prefix of an uncollected study family to include (e.g. S-BSST); repeatable")
    parser.add_argument("--concurrency", type=int, default=8)
    parser.add_argument("--page-size", type=int, default=100)
    parser.add_argument("--max-failure-fraction", type=float, default=0.005,
                        help="Fail the download if more than this fraction of studies could not be fetched")
    parser.add_argument("dest", type=Path, help="Directory to populate with <ACC>/<ACC>.json")
    args = parser.parse_args(argv)
    if not args.collection and not args.accession_prefix:
        parser.error("at least one --collection or --accession-prefix is required")

    accessions = enumerate_accessions(args.api, args.collection, args.accession_prefix, args.page_size)
    if not accessions:
        print("ERROR: no studies enumerated", file=sys.stderr)
        return 1
    print(f"{len(accessions)} distinct studies to fetch into {args.dest}", file=sys.stderr)

    removed = prune(args.dest, accessions)
    counts = fetch_all(args.api, args.dest, accessions, args.concurrency)
    print(f"done: {counts}, pruned {removed} withdrawn", file=sys.stderr)

    if counts["failed"] > args.max_failure_fraction * len(accessions):
        print(f"ERROR: {counts['failed']} of {len(accessions)} studies failed to fetch", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
