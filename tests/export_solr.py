#!/usr/bin/env python3
"""Export Solr cores to JSONL snapshot files."""

import json
import sys
import urllib.request


def export_core(core_name, sort_field, output_file):
    rows_per_page = 500
    start = 0
    all_docs = []

    while True:
        url = f"http://localhost:8983/solr/{core_name}/select?q=*:*&rows={rows_per_page}&start={start}&wt=json"
        if sort_field:
            url += f"&sort={sort_field}+asc"
        try:
            with urllib.request.urlopen(url) as resp:
                data = json.loads(resp.read())
            docs = data.get("response", {}).get("docs", [])
            if not docs:
                break
            all_docs.extend(docs)
            start += rows_per_page
            if start >= data["response"]["numFound"]:
                break
        except Exception as e:
            print(f"Error fetching from {core_name}: {e}", file=sys.stderr)
            if sort_field:
                print("Retrying without sort...", file=sys.stderr)
                return export_core(core_name, None, output_file)
            break

    with open(output_file, "w") as f:
        for doc in sorted(all_docs, key=lambda d: json.dumps(d, sort_keys=True)):
            for key in list(doc.keys()):
                if key.startswith("_") and key.endswith("_"):
                    del doc[key]
            print(json.dumps(doc, sort_keys=True, ensure_ascii=False), file=f)

    return len(all_docs)


if __name__ == "__main__":
    subgraph = sys.argv[1]

    n = export_core(f"grebi_nodes_{subgraph}", "grebi__nodeId", f"{subgraph}_snapshot_solr_nodes.jsonl")
    print(f"Exported {n} Solr nodes")
