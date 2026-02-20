#!/usr/bin/env python3
"""Export Neo4j nodes and edges to JSONL snapshot files."""

import json
import sys
import urllib.request


def run_cypher(neo4j_url, query):
    """Execute a Cypher query via Neo4j HTTP API and return results."""
    url = f"{neo4j_url}/db/neo4j/tx/commit"
    payload = json.dumps({
        "statements": [{
            "statement": query,
            "resultDataContents": ["row"]
        }]
    }).encode()
    req = urllib.request.Request(url, data=payload, headers={
        "Content-Type": "application/json",
        "Accept": "application/json"
    })
    with urllib.request.urlopen(req) as resp:
        result = json.loads(resp.read())
    if result.get("errors"):
        print(f"Neo4j error: {result['errors']}", file=sys.stderr)
        sys.exit(1)
    return result["results"][0]


def export_nodes(neo4j_url, subgraph):
    print("Exporting Neo4j nodes...", file=sys.stderr)
    result = run_cypher(neo4j_url, """
        MATCH (n:GraphNode)
        WITH n ORDER BY n.`grebi:nodeId`
        RETURN n.`grebi:nodeId` AS nodeId, labels(n) AS labels, properties(n) AS props
    """)
    columns = result["columns"]
    out_file = f"{subgraph}_snapshot_neo4j_nodes.jsonl"
    with open(out_file, "w") as f:
        for row_data in result["data"]:
            row = dict(zip(columns, row_data["row"]))
            props = row.get("props", {})
            for key in list(props.keys()):
                if key in ("num_desc", "ic"):
                    del props[key]
            output = {
                "nodeId": row["nodeId"],
                "labels": sorted(row.get("labels", [])),
                "properties": props
            }
            print(json.dumps(output, sort_keys=True, ensure_ascii=False), file=f)
    print(f"Exported {len(result['data'])} nodes", file=sys.stderr)


def export_edges(neo4j_url, subgraph):
    print("Exporting Neo4j edges...", file=sys.stderr)
    result = run_cypher(neo4j_url, """
        MATCH (a:GraphNode)-[r]->(b:GraphNode)
        WITH a, r, b ORDER BY a.`grebi:nodeId`, type(r), b.`grebi:nodeId`
        RETURN a.`grebi:nodeId` AS fromId, type(r) AS relType, b.`grebi:nodeId` AS toId, properties(r) AS props
    """)
    columns = result["columns"]
    out_file = f"{subgraph}_snapshot_neo4j_edges.jsonl"
    with open(out_file, "w") as f:
        for row_data in result["data"]:
            row = dict(zip(columns, row_data["row"]))
            output = {
                "from": row["fromId"],
                "properties": row.get("props", {}),
                "to": row["toId"],
                "type": row["relType"]
            }
            print(json.dumps(output, sort_keys=True, ensure_ascii=False), file=f)
    print(f"Exported {len(result['data'])} edges", file=sys.stderr)


if __name__ == "__main__":
    subgraph = sys.argv[1]
    neo4j_url = sys.argv[2] if len(sys.argv) > 2 else "http://127.0.0.1:7474"
    export_nodes(neo4j_url, subgraph)
    export_edges(neo4j_url, subgraph)
