#!/usr/bin/env python3
"""Snapshot every released EGA study's public metadata as one gzipped JSONL file.

A one-off (or occasional) tool, not part of the pipeline, like the BioStudies
snapshot: the snapshot is published on the SPOT FTP area, and whatever reads EGA
(the Multiomic Explorer; GrEBI has no EGA datasource) reads that file rather than
EGA's API.

EGA publishes no dump of its metadata, but its metadata API answers a listing of
every study in one request when the page asked for is larger than the archive, so
the snapshot is a single request, never a crawl. A page that comes back full may
have been cut short, and is refused rather than written. Studies not yet released,
and deprecated ones, are left out; the rest are written one per line, as the API
gives them, sorted by accession.

    python3 ega_snapshot.py ega_studies.jsonl.gz
"""

import argparse
import gzip
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

USER_AGENT = "GrEBI snapshot (https://github.com/EBISPOT/GrEBI)"
API = "https://metadata.ega-archive.org"
# Far more than the ~11k studies the archive holds in 2026; a full page means it
# has outgrown this, and the snapshot is refused.
DEFAULT_LIMIT = 1_000_000


def http_get(url: str, attempts: int = 5) -> bytes:
    """GET with retries and backoff; a 4xx other than 429 fails at once."""
    delay = 2.0
    for attempt in range(1, attempts + 1):
        try:
            request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT, "Accept": "application/json"})
            with urllib.request.urlopen(request, timeout=600) as response:
                return response.read()
        except urllib.error.HTTPError as error:
            last = f"HTTP {error.code}"
            if 400 <= error.code < 500 and error.code != 429:
                raise RuntimeError(f"{url}: {last}")
        except (urllib.error.URLError, TimeoutError, OSError) as error:
            last = str(error)
        if attempt == attempts:
            raise RuntimeError(f"{url}: giving up after {attempts} attempts ({last})")
        time.sleep(delay)
        delay = min(delay * 2, 60.0)


def list_studies(api: str, limit: int, get=http_get) -> list[dict]:
    """Every study, in one request; refused if the page is full, as it may be cut short."""
    url = f"{api}/studies?{urllib.parse.urlencode({'limit': limit})}"
    studies = json.loads(get(url))
    if not isinstance(studies, list):
        raise ValueError(f"{url}: expected a JSON list of studies, got {type(studies).__name__}")
    if len(studies) >= limit:
        raise ValueError(f"{url}: {len(studies)} studies fill the page, so it may be cut short; raise --limit")
    for study in studies:
        if not isinstance(study, dict) or not isinstance(study.get("accession_id"), str):
            raise ValueError(f"{url}: a study without an accession_id: {json.dumps(study)[:200]}")
    return studies


def released(studies: list[dict]) -> list[dict]:
    """The released, current studies, by accession."""
    kept = [s for s in studies if s.get("is_released") is True and s.get("is_deprecated") is not True]
    accessions = [s["accession_id"] for s in kept]
    if len(set(accessions)) != len(accessions):
        twice = sorted({a for a in accessions if accessions.count(a) > 1})
        raise ValueError(f"studies listed twice: {twice[:10]}")
    return sorted(kept, key=lambda s: s["accession_id"])


def write_snapshot(studies: list[dict], output: Path) -> int:
    """One compact study per line; written under a temporary name and renamed into place."""
    tmp = output.with_name(output.name + ".part")
    with gzip.open(tmp, "wt", encoding="utf-8") as out:
        for study in studies:
            out.write(json.dumps(study, ensure_ascii=False, separators=(",", ":"), sort_keys=True))
            out.write("\n")
    os.replace(tmp, output)
    return len(studies)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--api", default=API)
    parser.add_argument("--limit", type=int, default=DEFAULT_LIMIT,
                        help="page size asked for; must exceed the number of studies")
    parser.add_argument("output", type=Path, help="the .jsonl.gz to write")
    args = parser.parse_args(argv)

    studies = list_studies(args.api, args.limit)
    kept = released(studies)
    if not kept:
        print("ERROR: no released study listed", file=sys.stderr)
        return 1
    written = write_snapshot(kept, args.output)
    print(f"{len(studies)} studies listed, {written} released written to {args.output}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
