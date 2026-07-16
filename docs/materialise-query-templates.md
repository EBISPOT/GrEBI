# Design: fold `materialised_queries` into `query_templates`

Status: **design / not yet implemented.** Author notes from an investigation + a
full materialisation-sizing experiment on the 2026-07-08 `ebi_monarch_xspecies`
snapshot (see [Appendix A](#appendix-a-validated-materialisation-sizes)).

## Goal

Eliminate the separate `materialised_queries/` concept and make it a capability of
`query_templates/`. A query template should be able to be **materialised** —
precomputed into Postgres at dataload — so that at query time most templates are
served by a cheap indexed Postgres lookup instead of live Cypher.

## Why (motivation)

- Two parallel systems today (see [Current state](#current-state)) with **zero
  shared code** — duplicated formats, pipelines, serving, and UI. One concept is
  simpler to author and maintain.
- Live Cypher counts are the latency villain for the wide GWAS templates
  (measured 15–108 s cold; see the earlier perf work), and cold-cache tails are
  brutal (`disease_to_genes` first page 85 s cold → 0.2 s warm). Precomputed
  tables give flat, predictable latency and take Neo4j off the hot path.
- We now have **measured sizes** for materialising every template (Appendix A):
  almost all are ≤ ~13 M rows and build in minutes — materialisation is viable.

## Current state (what exists)

Two fully separate subsystems:

| aspect | `query_templates` | `materialised_queries` |
|---|---|---|
| definition | YAML on disk, `GREBI_QUERY_TEMPLATES_PATH` (rich: fragments, params, result_columns, examples, topics) | YAML in `materialised_queries/` (`cypher_query`, `run_for_subgraphs`, `uses_datasources`, `title`, `description`) |
| dataload | copied into release tarball only; **never precomputed** (`construct_release.nf:11,38`) | run at dataload (`run_queries.dockerpy`) → link → pgbin → table |
| storage | none (live) | Postgres `materialised_queries_{sg}` (`query_id TEXT, row_number INT, data JSONB`) |
| serving | live Cypher via `GrebiCypherRepo` (paginated + streamed CSV) | Postgres search `GrebiPostgresClient.searchMaterialisedQueryResults` (text/filter/facet over JSONB) |
| API | `/graphs/{g}/query_templates[/{id}]`, `/graphs/{g}/query/{id}[.csv]` | `/materialised_queries`, `/graphs/{g}/materialised_queries/{id}` |
| defs listed from | the repo (filesystem) | `graph_metadata.materialised_queries` JSON |
| UI | `/queries` — `QueryInterface`, param autocomplete via `param_opts`→`/search` | `/tables` — `MaterialisedQueryTable` + browsable `ResultsTable` |
| model | rich `QueryTemplate.java` / `.ts` | thin `{graph, id}` + freeform metadata |

### The generic, reusable dataload path

The dataload flow for a materialised query is mostly **generic** and can be reused
verbatim for template materialisation:

```
yaml (cypher_query)
  → run_queries.dockerpy         run Cypher vs Neo4j, one {id}.results.jsonl per query
  → grebi_link_results           enrich each row: add _refs (resolved node metadata),
                                  _node_ids, id (sha1 of the row) — so the UI renders
                                  results without re-querying
  → grebi_make_postgres_mat_queries   → PGCOPY binary (query_id, row_number, data JSONB)
  → load_postgres.py             CREATE TABLE materialised_queries_{sg} + COPY FREEZE
                                  + btree(query_id), btree(query_id,row_number)
  metadata branch:
  run_queries.dockerpy metadata (queries.json)
  → add_query_metadatas_to_graph_metadata.py   graph_metadata['materialised_queries'] = [...]
```

Key files: `dataload/07_run_queries/run_queries.dockerpy`,
`.../grebi_link_results/src/main.rs`, `.../add_query_metadatas_to_graph_metadata.py`,
`dataload/08_create_postgres/grebi_make_postgres_mat_queries/src/main.rs`,
`dataload/nextflow/processes/08_create_postgres/load_postgres.py:162-163,291-293`.
Yaml dir is hardwired to `$GREBI_HOME/materialised_queries` in
`dataload/scripts/dataload_*.sh` → `params.query_yamls_path` (`main.nf:37`).

## The hard part: materialisation is NOT "free the param"

**This is the crux and the most expensive lesson of the investigation.** Naively
materialising a parameterised template by removing the `{id: $param}` anchor and
counting the result set is wrong on two counts:

1. **Domain constraints live only in `param_opts`** (a UI autocomplete hint, fed to
   `/search`), not in the Cypher. Freeing the param lets the anchor roam the whole
   graph. Measured impact: `disease_to_processes` went from a faithful **178 k** rows
   to a bogus **20 M** because the freed `disease` matched non-disease nodes that
   happened to have process edges. Fix: constrain the freed node to its domain —
   a **label** for type roots (`hgnc:Gene`, `impc:MouseGene`, `biolink:ChemicalEntity`)
   or an **Id-indexed broad_match** for ontology-CURIE roots
   (`(:Id {id:'mondo:0000001'})<-[:sourceId]-(root)<-[:biolink:broad_match*0..1]-(var)`).
   (`biolink:broad_match` is a precomputed transitive closure, so a single `*0..1`
   hop reaches all ancestors.)

2. **Roll-up templates blow up if you enumerate the `_root`.** Templates like
   `gwas_by_cell_type` bind the queried node as `cl_root` and roll a base term up to
   it (`WHERE cl_term = cl_root OR (cl_term)->broad_match->(cl_root)`), with the
   **base term (`cl_term`), not `cl_root`, in the `DISTINCT`**. Freeing `cl_root` and
   keeping the roll-up makes it enumerate all 6,172 cells and regenerate every
   `(gene, cl_term, trait, snp)` row once per cell-type ancestor — a quadratic
   intermediate that `DISTINCT` then throws away. Result: **12 h timeout** even on a
   256 GB-heap node. The correct materialisation **keys on the base term and drops
   the `_root` enumeration**, constraining the base term to its domain directly:

   ```cypher
   // WRONG (my first transform): enumerate cl_root, roll cl_term up to it → 12h timeout
   // RIGHT: key on cl_term, do the cell-closure at SERVING time
   MATCH (cl_term)-[:`biolink:broad_match`*0..1]->(dr)-[:sourceId]->(:Id {id:'cl:0000000'})
   MATCH (cl_term)<-[r:(...)]-(matched_trait)
   ... WITH DISTINCT cl_term, trait, snp, gene, study, or_or_beta, edge_id RETURN count(*)
   ```

   Same query, base-keyed: **1.47 M rows in ~2–13 min** instead of 12 h. Confirmed
   for all four fan-outs (Appendix A).

**General principle: materialise by keying on the base node the row is *about*
(`specific_disease`, `cl_term`, `location`), constrain it to its `param_opts`
domain, and do the ancestor/descendant closure at SERVING time** (in Postgres,
using the precomputed `broad_match` closure) — never by enumerating the queried
`_root` and expanding the closure into storage.

**Consequence for this design:** the materialised form of a parameterised template
is a genuinely different query that is not reliably auto-derivable. Each
materialisable parameterised template must **provide** its base-keyed materialise
query, and the serving layer must do closure-at-query-time. (The base-keyed forms
for the hard templates already exist — Appendix B.)

## Target architecture

One YAML format in `query_templates/`. A template is one of three kinds:

1. **Live parameterised** (unchanged): fragments + params, served via Cypher.
2. **Standalone materialised** (= today's `materialised_queries`): a `cypher_query`
   with no params; always precomputed; served as a browsable table.
3. **Materialised parameterised** (the end goal): a live parameterised template that
   *also* carries a `materialise:` block (the base-keyed query + the serving key
   column). Precomputed and served from Postgres with closure-at-query-time; falls
   back to live Cypher if not built.

### Unified YAML schema (superset)

```yaml
# --- always ---
title: ...
description: ...
graphs: [ebi_monarch_xspecies]
topics: [...]

# --- live parameterised (kinds 1 & 3) ---
question: "What [genes]{gene} are associated with {disease_id}?"
cypher_match_fragment: |- ...
cypher_return_fragment: |- ...
cypher_count_fragment: |- ...
params: [ {param_id, param_name, param_type, param_default?, param_opts?} ]
result_columns: [ {column_id, column_type, optional?} ]
examples: [ {title, params} ]

# --- materialisation (kinds 2 & 3), optional ---
materialise:
  # kind 2 (standalone): a full query, no params
  cypher: |- ...
  # kind 3 (parameterised): the base-keyed, domain-constrained, param-freed query
  #   producing all rows, PLUS the serving key column(s) and how the param maps to them
  serving_key: { column: disease, closure: descendants }   # given queried X, match rows whose `disease` ∈ descendants(X)
  run_for_subgraphs: [impc_x_gwas]     # from mat queries (optional)
uses_datasources: [IMPC, GWAS]         # display only (optional)
```

Notes:
- Auto-materialise rule from the original proposal — "one param without a default
  and no `materialise: false`" — is **not** sufficient on its own precisely because
  of the base-keying problem. Keep an explicit `materialise:` block. The rule can
  still gate *whether* a template is a candidate, but the block supplies the *how*.
- A **build-size budget** must gate the pipeline (refuse/warn above N M rows) so a
  new template can't silently 10× the dataload (Appendix A shows the range).

## Implementation plan

### Stage 1 — unify the concept (fold standalone mat queries in) — ~1 day, low risk

Satisfies the literal ask ("get rid of materialised_queries") without the hard
serving work.

Dataload:
- Move `materialised_queries/hello_world.yaml`, `impc_x_gwas.yaml` into
  `query_templates/` as **standalone-materialised** templates (`materialise.cypher`
  = the old `cypher_query`; carry `run_for_subgraphs`, `uses_datasources`).
- Point the `run_materialised_queries` step at `query_templates/`, selecting only
  templates that have a `materialise` block (skip live-only templates). Update
  `run_queries.dockerpy` to read `template['materialise']['cypher']` and honour
  `run_for_subgraphs`. Everything downstream (link → pgbin → `materialised_queries_{sg}`
  → `graph_metadata`) is unchanged.
- Update the yaml-dir wiring in `dataload/scripts/dataload_*.sh` (`GREBI_QUERY_YAMLS_PATH`)
  to point at `query_templates` (or drop it — reuse the templates path).
- Delete `materialised_queries/`.

Webapp:
- `/tables` list + serving is unchanged on the backend (still reads
  `graph_metadata.materialised_queries` + the `materialised_queries_{sg}` table).
  Only the *source* of the definitions changed (now authored in `query_templates/`),
  which is transparent to the API.
- Optional: fix the `MaterialisedQueryTable.tsx` GitHub link (currently hardcoded to
  `materialised_queries/{id}.yaml`) to point at the new template path.

Result: one authoring dir, one pipeline entry, `materialised_queries/` gone.
Parameterised templates still served live.

### Stage 2 — materialise parameterised templates + serve from Postgres — multi-day

The user's end goal ("don't do cypher at query time for most templates").

Dataload:
- For each parameterised template with a `materialise:` block, run its base-keyed
  `materialise.cypher` (Appendix B) through the same link→pgbin→table path. The
  serving key column(s) must be present in the stored `data` (they already are —
  the base node is a result column).
- Enforce the build-size budget.

Serving (the **new** component — closure-at-query-time):
- New endpoint / branch that, given a materialised parameterised template + a param
  value P, returns the stored rows whose serving-key column is in `closure(P)`
  (descendants or self, via the precomputed `broad_match` closure available in
  Postgres as node properties / an edges table). Concretely: resolve P → its
  descendant base-node ids → `SELECT data FROM materialised_queries_{sg} WHERE
  query_id = ... AND data->key->>'id' = ANY(descendant_ids)` (+ paging, the same
  text/filter/facet machinery already in `searchMaterialisedQueryResults`).
- Route the existing `/graphs/{g}/query/{id}` and `.csv` endpoints to the Postgres
  path when a built materialisation exists, else fall back to `GrebiCypherRepo`.
- Counts become `count(*)` over the filtered rows — flat and cheap (kills the
  15–108 s live count problem).

UI:
- The `/queries` interactive UI can stay identical (it just calls the same
  `/query/{id}` endpoint, now Postgres-backed). Optionally merge `/tables` and
  `/queries` sections since they become one concept.

### Also worth doing (from the perf work, orthogonal but related)

- **Bake the `param_opts` domain into the template Cypher** (a label for type roots,
  a `broad_match` filter for ontology roots) so the live query is robust to direct
  API calls and materialisation is trivial. The four `gwas_by_gene_*` /
  `gene_to_diseases` templates were already given the `(gene:\`hgnc:Gene\`)` label
  (behaviour-preserving — verified). See the perf review.
- **Counts-only precompute** for any template too big to fully materialise: store one
  integer per param value (cheap), which alone kills the live-count latency.

## Migration / compatibility

- `graph_metadata.materialised_queries` stays as the list source in Stage 1, so the
  API and `/tables` UI need no change. The `/materialised_queries` array-nesting
  "botched dataload" hack (`GrebiApi.java:265-269`) can be removed once rebuilt.
- The MCP resource `grebi://query_templates` and the docs/snapshot integration tests
  (`test_query_templates.nf`) should be extended to cover materialised templates.
- One snapshot detail: the dev/experiment snapshots predate a datasource-tag rename
  (`OLS.*` → `Ontologies.*`); templates use the new `Ontologies.*`. Not relevant to
  this refactor except when testing against old snapshots.

## Open questions

1. Do we merge the `/tables` and `/queries` UI sections, or keep "browsable table"
   vs "interactive query" as two presentations of one underlying concept?
2. Serving-key closure direction is per-template (descendants for
   disease/cell/location "or subclasses", but note some templates roll *up*). The
   `materialise.serving_key.closure` field must capture this; validate per template.
3. Build-size budget threshold and what happens on exceed (skip + warn, or fail).
4. `disease_to_genes` / `gene_to_diseases` are the genuine ~100 M-edge tables — decide
   materialise vs counts-only vs keep-live once the codon run reports their true size.

---

## Appendix A: validated materialisation sizes

Measured on the 2026-07-08 `ebi_monarch_xspecies` snapshot (94.9 M nodes, 1.09 B
edges). "constrained" = domain-constrained; fan-outs use the base-keyed form.

| template | rows | build | note |
|---|--:|--:|---|
| materialised_queries/hello_world | 1 | 1 s | |
| hello_world | 8 | 18 s | test graph |
| mouse_gene_to_opentargets | 0 | 1 s | `otar:*` edges absent in snapshot |
| disease_to_exposures | 78,587 | 38 s | |
| disease_to_treatments | 254,786 | 18 s | |
| disease_to_processes | 178,261 | 3 s | was 20 M unconstrained (domain fix) |
| disease_to_locations | 498,428 | 4 s | was 1.9 M unconstrained |
| disease_to_phenotypes | 429,958 | 12 min | was 1.17 M unconstrained |
| gwas_by_drugs_indicated_for_disease | 21,387 | 38 s | |
| chebi_to_metabolights | 365,080 | 2.7 min | already label-constrained |
| phenotype_to_diseases | 4,852,669 | 33 s | no domain constraint (correct) |
| gwas_by_pathway | 668,454 | 8 min | |
| gwas_traits_reported_different | 797,294 | 5 min | |
| gwas_by_gene_and_cell_type | 1,254,623 | 94 s | **base-keyed** (was 12 h timeout) |
| gwas_by_cell_type | 1,471,331 | 13 min | **base-keyed** (was 12 h timeout) |
| gwas_by_disease_location | 6,651,462 | 11 min | **base-keyed** (was 12 h timeout) |
| gwas_by_location | 12,869,019 | 20 min | **base-keyed** (was 12 h timeout) |
| gwas_by_gene_and_disease | 3,753,465 | 47 min | |
| gwas_by_gene_and_location | 8,067,406 | 72 min | |
| gene_to_diseases | TBD | — | genuine ~125 M-edge table (running) |
| disease_to_genes | TBD | — | genuine ~125 M-edge table (running) |
| gwas_trait_to_mouse_models_via_embeddings | TBD | — | KNN; special-case |

Takeaway: **~18 of ~21 materialise in ≤ ~20 min at ≤ ~13 M rows.** Only the two
gene↔disease association tables are potentially too large; the embeddings query is a
special case.

## Appendix B: base-keyed materialise queries (the hard four)

The corrected, base-keyed, domain-constrained count queries validated on the
snapshot (drop the `_root` enumeration; constrain the base term to its domain). See
the experiment at `/nfs/production/parkinso/spot/jmcl/grebi_query_timings` on codon
(`queries.jsonl` holds the exact bodies; `logs/results.tsv` the results). Reproduce
`RETURN count(*)` → materialise by swapping to the template's `RETURN DISTINCT ...`.

- `gwas_by_cell_type`: key on `cl_term`, `MATCH (cl_term)-[:biolink:broad_match*0..1]->(dr)-[:sourceId]->(:Id {id:'cl:0000000'})`, drop `cl_root`.
- `gwas_by_location`, `gwas_by_disease_location`: key on `location`, constrain to `uberon:0001062`, drop `location_root`.
- `gwas_by_gene_and_cell_type`: `(gene:\`hgnc:Gene\`)` + key on `cl_term` constrained to `cl:0000000`, drop `cl_root`.
