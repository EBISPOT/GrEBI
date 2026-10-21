#!/usr/bin/env python3
"""Discover ai_anno's published annotation tables from its latest.txt manifest.

ai_anno (the LLM annotation pipeline for free-text study metadata) publishes
each project's result tables to the SPOT FTP under
https://ftp.ebi.ac.uk/pub/databases/spot/ai_anno/<project>/<release>/, and
latest.txt at the root lists the URL of every file of each project's latest
release, one per line. This reads that local copy and emits ordinary GrEBI
download entries, one per table, saved as ai_anno/<project>/<table>, so the
AI_anno ingest finds every current table without a file list in YAML. It
makes no network requests.
"""

import argparse
import json
import re
import sys

TABLE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]*_annotations\.(tsv|csv)(\.gz)?$")
PROJECT = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]*$")


def download_entries(manifest: str) -> list[dict]:
    entries = {}
    for line in manifest.splitlines():
        source = line.strip()
        if not source or source.startswith("#"):
            continue
        # .../<project>/<release>/<table>: the release directory is the dated
        # one latest points at, so the project is two levels up from the file
        parts = source.rstrip("/").split("/")
        if len(parts) < 3:
            raise ValueError(f"not a <project>/<release>/<table> path: {source}")
        project, table = parts[-3], parts[-1]
        if not PROJECT.fullmatch(project):
            raise ValueError(f"invalid project directory in {source}")
        if not TABLE.fullmatch(table):
            raise ValueError(f"not an annotations table: {source}")
        dest = f"ai_anno/{project}/{table}"
        if dest in entries:
            raise ValueError(f"{dest} is listed twice")
        entries[dest] = {"dest": dest, "sources": [source]}
    if not entries:
        raise ValueError("the manifest lists no annotations tables")
    return [entries[dest] for dest in sorted(entries)]


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("manifest", type=argparse.FileType("r"), help="The local copy of latest.txt")
    args = parser.parse_args()
    try:
        entries = download_entries(args.manifest.read())
        json.dump(entries, sys.stdout, indent=2)
        sys.stdout.write("\n")
        print(f"Discovered {len(entries)} annotations tables", file=sys.stderr)
    except ValueError as error:
        parser.exit(1, f"Invalid ai_anno manifest: {error}\n")
