#!/usr/bin/env python3
"""Snapshot every non-Europe-PMC BioStudies study as one gzipped JSONL file.

A one-off (or occasional) tool, not part of the pipeline: the pipeline downloads
the published snapshot from the SPOT FTP area, the way it does for MGnify, so a
dataload makes no BioStudies API requests at all.

Studies are enumerated through the search API by collection and by accession
prefix (the uncollected S-* families have no collection to query), fetched with
bounded concurrency into a scratch directory (so an interrupted run resumes),
then written one PageTab document per line, sorted by accession. The API
document is byte-identical to the FTP tree's <ACC>/<ACC>.json.

    python3 biostudies_snapshot.py --scratch /tmp/biostudies biostudies.jsonl.gz
"""

import argparse
import concurrent.futures
import gzip
import json
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

USER_AGENT = "GrEBI snapshot (https://github.com/EBISPOT/GrEBI)"
ACCESSION = re.compile(r"[A-Z]-[A-Z]{3,4}-?\d+")
EUROPE_PMC_PREFIX = "S-EPMC"

# Everything BioStudies holds apart from Europe PMC: the named collections, and
# the accession prefixes of the families that are not in a collection.
DEFAULT_COLLECTIONS = ["ArrayExpress", "BioImages", "BioImages-EMPIAR", "SourceData"]
DEFAULT_PREFIXES = ["S-BSST", "S-BIAD", "S-CMO", "S-VHPS", "S-MBRS", "S-ONTX", "S-RHER", "S-AIMD", "S-BAIR"]


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


# The search API serves at most this many results per query, whatever the page
# size or sort order (HTTP 500 beyond), so a bigger query is sliced by release
# year, which the API exposes as a facet.
RESULT_WINDOW = 20000
FIRST_RELEASE_YEAR = 2000


def paginate(api: str, params: dict, page_size: int, fetch=search):
    """Every hit of a search, page by page, slicing by release year when it exceeds the window."""
    first = fetch(api, {**params, "pageSize": page_size, "page": 1})
    total = int(first.get("totalHits") or 0)
    if total > RESULT_WINDOW:
        if "facet.released_year" in params:
            raise RuntimeError(f"{params}: {total} results, more than the API will page through")
        for year in range(FIRST_RELEASE_YEAR, time.gmtime().tm_year + 2):
            yield from paginate(api, {**params, "facet.released_year": str(year)}, page_size, fetch)
        return
    page = 1
    seen = 0
    result = first
    while True:
        hits = result.get("hits") or []
        yield from hits
        seen += len(hits)
        if not hits or seen >= total:
            return
        page += 1
        result = fetch(api, {**params, "pageSize": page_size, "page": page})


def enumerate_accessions(api: str, collections: list[str], prefixes: list[str],
                         page_size: int = 1000, fetch=search) -> list[str]:
    """Distinct non-Europe-PMC accessions across the collections and accession prefixes."""
    accessions: set[str] = set()

    def keep(accession: str) -> bool:
        return bool(ACCESSION.fullmatch(accession)) and not accession.startswith(EUROPE_PMC_PREFIX)

    for collection in collections:
        found = [h.get("accession", "") for h in paginate(api, {"collection": collection}, page_size, fetch)]
        found = [a for a in found if keep(a)]
        accessions.update(found)
        print(f"collection {collection}: {len(found)} studies", file=sys.stderr)
    for prefix in prefixes:
        # The query is a free-text search, so it returns a superset: keep only
        # accessions that really carry the prefix.
        found = [h.get("accession", "") for h in paginate(api, {"query": f"{prefix}*"}, page_size, fetch)]
        found = [a for a in found if a.startswith(prefix) and keep(a)]
        accessions.update(found)
        print(f"prefix {prefix}: {len(found)} studies", file=sys.stderr)
    return sorted(accessions)


def fetch_study(api: str, scratch: Path, accession: str, get=http_get) -> str:
    """Fetch one study into scratch unless already there. 'kept', 'fetched' or 'missing'."""
    path = scratch / f"{accession}.json"
    if path.is_file() and path.stat().st_size > 0:
        return "kept"
    body = get(f"{api}/studies/{accession}")
    if body is None:
        return "missing"
    document = json.loads(body)  # a truncated or HTML response must not be kept
    if not isinstance(document, dict):
        raise ValueError(f"{accession}: expected a JSON object")
    tmp = path.with_suffix(".json.part")
    tmp.write_bytes(body)
    os.replace(tmp, path)
    return "fetched"


def fetch_all(api: str, scratch: Path, accessions: list[str], concurrency: int, get=http_get) -> dict[str, int]:
    counts = {"kept": 0, "fetched": 0, "missing": 0, "failed": 0}
    scratch.mkdir(parents=True, exist_ok=True)
    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as pool:
        futures = {pool.submit(fetch_study, api, scratch, accession, get): accession for accession in accessions}
        for done, future in enumerate(concurrent.futures.as_completed(futures), 1):
            try:
                counts[future.result()] += 1
            except Exception as error:  # one bad study must not stop the other 100k
                counts["failed"] += 1
                print(f"WARNING: {futures[future]}: {error}", file=sys.stderr)
            if done % 5000 == 0:
                print(f"{done}/{len(accessions)} studies: {counts}", file=sys.stderr)
    return counts


def write_snapshot(scratch: Path, accessions: list[str], output: Path) -> int:
    """One compact PageTab document per line, in accession order; missing studies are skipped."""
    written = 0
    tmp = output.with_name(output.name + ".part")
    with gzip.open(tmp, "wt", encoding="utf-8") as out:
        for accession in accessions:
            path = scratch / f"{accession}.json"
            if not path.is_file():
                continue
            document = json.loads(path.read_text(encoding="utf-8"))
            out.write(json.dumps(document, ensure_ascii=False, separators=(",", ":")))
            out.write("\n")
            written += 1
    os.replace(tmp, output)
    return written


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--api", default="https://www.ebi.ac.uk/biostudies/api/v1")
    parser.add_argument("--collection", action="append", help="collection to include; repeatable (default: all known)")
    parser.add_argument("--accession-prefix", action="append",
                        help="prefix of an uncollected study family to include; repeatable (default: all known)")
    parser.add_argument("--scratch", type=Path, required=True, help="directory of per-study JSON, kept between runs")
    parser.add_argument("--concurrency", type=int, default=8)
    parser.add_argument("--page-size", type=int, default=1000)
    parser.add_argument("--max-failure-fraction", type=float, default=0.005,
                        help="fail if more than this fraction of studies could not be fetched")
    parser.add_argument("output", type=Path, help="the .jsonl.gz to write")
    args = parser.parse_args(argv)
    collections = args.collection if args.collection is not None else DEFAULT_COLLECTIONS
    prefixes = args.accession_prefix if args.accession_prefix is not None else DEFAULT_PREFIXES

    accessions = enumerate_accessions(args.api, collections, prefixes, args.page_size)
    if not accessions:
        print("ERROR: no studies enumerated", file=sys.stderr)
        return 1
    print(f"{len(accessions)} distinct studies", file=sys.stderr)

    counts = fetch_all(args.api, args.scratch, accessions, args.concurrency)
    print(f"fetched: {counts}", file=sys.stderr)
    if counts["failed"] > args.max_failure_fraction * len(accessions):
        print(f"ERROR: {counts['failed']} of {len(accessions)} studies failed to fetch; rerun to retry them", file=sys.stderr)
        return 1

    written = write_snapshot(args.scratch, accessions, args.output)
    print(f"wrote {written} studies to {args.output}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
