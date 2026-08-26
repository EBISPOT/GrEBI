
import json
import sys


def main():
    graph_metadata_filename = sys.argv[1]
    query_metadata_filenames = sys.argv[2:]

    with open(graph_metadata_filename, 'r') as file:
        graph_metadata = json.load(file)

    # Standalone materialised queries (browsable tables, the old
    # `materialised_queries/` concept) are listed under `materialised_queries`
    # and served by the /tables UI. Parameterised materialised templates record
    # their build under `materialised_templates` so the API can route
    # /query/{id} to the Postgres closure path (they are still authored + shown
    # as interactive query templates).
    materialised_queries = []
    materialised_templates = []

    # Parameterised materialised templates are a serving/routing descriptor, so we
    # keep only stable fields (no run-dependent num_results / timings): the API
    # needs `id` to route, mode/params for the closure filter, and table/columns
    # to address + project the typed storage table.
    stable_template_fields = ["id", "subgraph", "title", "kind", "mode", "params",
                              "table", "columns"]

    for query_metadata_filename in query_metadata_filenames:
        with open(query_metadata_filename, 'r') as file:
            query_metadata = json.load(file)

        # queries.json holds a list of per-query metadata dicts.
        entries = query_metadata if isinstance(query_metadata, list) else [query_metadata]
        for q in entries:
            if not isinstance(q, dict):
                continue
            if q.get("kind") == "parameterised":
                materialised_templates.append(
                    {k: q[k] for k in stable_template_fields if k in q})
            else:
                # Standalone: browsable-table metadata; drop the internal routing
                # marker so the entry keeps its original display shape.
                q.pop("kind", None)
                materialised_queries.append(q)

    graph_metadata['materialised_queries'] = materialised_queries
    graph_metadata['materialised_templates'] = materialised_templates

    print(json.dumps(graph_metadata, indent=2))


if __name__ == "__main__":
    main()
