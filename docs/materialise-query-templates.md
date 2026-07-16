# Design: fold `materialised_queries` into `query_templates`

Status: **implemented.** Stage 1 + Stage 2 are built; see
[Implementation status](#implementation-status) for exactly what landed, the final
YAML schema, and which templates are materialised vs kept live. The original
design rationale (from an investigation + a full materialisation-sizing experiment
on the 2026-07-08 `ebi_monarch_xspecies` snapshot, see
[Appendix A](#appendix-a-validated-materialisation-sizes)) is preserved below.

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

## The resolution: one body, the param is a closure root (no second Cypher)

The two lessons above do **not** mean a materialisable template needs a separate
`materialise` query. They mean the template should be authored in a **canonical
base-keyed form** where the `SourceId` param is expressed as the *closure root* of
the base column:

```cypher
MATCH (cl_term)-[:`biolink:broad_match`*0..1]->(x)-[:sourceId]->(:Id {id: $cell_type_id})
... rest of the body, keyed on cl_term ...
WITH DISTINCT cl_term, trait, snp, gene, study, or_or_beta, edge_id RETURN ...
```

This single body yields all three modes by only changing what `$cell_type_id`
binds to:

- **Live** — `$cell_type_id` = the queried cell. (`(base)-[:broad_match*0..1]->…->(:Id{queried})`
  = "base is the queried node or a descendant of it" — exactly today's semantics.)
- **Materialise** — substitute `$cell_type_id` = the `param_opts` domain root
  (`cl:0000000`). Run once, store keyed on `cl_term`. **Auto-derived — a literal
  substitution, no hand-written second query.**
- **Serve materialised** — given queried `C`, Postgres filters
  `data->'cell_type'->>'id' = ANY(descendants(C))` using the precomputed closure.

For **type-label** params (`hgnc:Gene`, `impc:MouseGene`, `biolink:ChemicalEntity`)
the base has no closure — the param is an exact match on a labelled node, so live =
`(gene:\`hgnc:Gene\`)-[:sourceId]->(:Id{$gene_id})` and materialise = `(gene:\`hgnc:Gene\`)`
(domain = the label). Same "substitute the root" mechanic.

### It is also *faster* live — measured

The unified base-keyed form isn't a performance compromise for the live path; it's a
**win**. Head-to-head on the snapshot for `gwas_by_cell_type`, single param, count query:

| queried cell | today's form (anchor + `subclass_of\|broad_match` rollup) | unified base-keyed form |
|---|--:|--:|
| leukocyte (high fan-out) | 398,142 in **25.1 s** | 398,142 in **12.3 s** |
| platelet | 48,383 in 2.8 s | 48,383 in 2.7 s |

Identical results; ~2× faster on the heavy case (the unified form goes straight
through the single precomputed `broad_match` closure hop instead of an intermediate
`cl_root` + a `subclass_of` alternative). **So rewriting the roll-up templates to the
base-keyed form is a standalone live-latency improvement — independent of
materialisation.**

**Consequence for this design:** no per-template second Cypher. The work is a
**one-time rewrite** of the parameterised templates into the canonical base-keyed /
param-as-closure-root form (Appendix B has the four hard ones), after which
materialise is a mechanical root-substitution and serving is a closure filter.

## Target architecture

One YAML format in `query_templates/`. A template is one of three kinds:

1. **Live parameterised** (unchanged): fragments + params, served via Cypher.
2. **Standalone materialised** (= today's `materialised_queries`): a `cypher_query`
   with no params; always precomputed; served as a browsable table.
3. **Materialised parameterised** (the end goal): a parameterised template authored
   in the canonical base-keyed form and flagged `materialise`. Its live body doubles
   as the materialise query (root-substituted); precomputed and served from Postgres
   with closure-at-query-time; falls back to live Cypher if not built.

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
materialise: true                      # opt in (or auto per the rule below)
run_for_subgraphs: [impc_x_gwas]       # from mat queries (optional)
uses_datasources: [IMPC, GWAS]         # display only (optional)

# per-param: which result column the param filters, and the closure direction.
# For a parameterised template authored in the canonical base-keyed form this is
# mostly derivable from param_opts (ontology-CURIE root => descendants of a base
# column; type-label root => exact match), but declare it explicitly to be safe:
params:
  - param_id: cell_type_id
    ...
    filters_column: cell_type          # the base column this param constrains
    closure: descendants               # descendants | ancestors | exact
```

Notes:
- **No separate `materialise.cypher`.** The materialise query is the template's own
  (canonical base-keyed) body with each param's closure root substituted by its
  `param_opts` domain root — a mechanical substitution done by the pipeline. Kind 2
  (standalone) is the degenerate case: no params, so the body *is* the materialise
  query.
- The auto-materialise rule from the original proposal — "one param without a
  default and no `materialise: false`" — can gate *whether* a template is
  materialised, but it only works once templates are in the canonical form (else it
  recreates the blow-up). So: rewrite to canonical form first, then the rule is safe.
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

Prerequisite (one-time):
- **Rewrite the parameterised templates into the canonical base-keyed /
  param-as-closure-root form.** This is also a live-latency improvement (2× on the
  `gwas_by_cell_type` stress case — see above), so it's worth doing regardless. The
  four roll-up templates are the main work (Appendix B); the disease-anchored ones
  just need the `param_opts` domain baked into the anchor.

Dataload:
- For each materialised template, derive the materialise query by substituting each
  param's closure root with its `param_opts` domain root, then run it through the
  same link→pgbin→table path. The serving key column(s) are already present in the
  stored `data` (the base node is a result column).
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
2. Per-param `closure` direction (descendants / ancestors / exact) and the
   `filters_column` it targets — mostly derivable from `param_opts`, but declare and
   validate per template.
3. Build-size budget threshold and what happens on exceed (skip + warn, or fail).
4. `disease_to_genes` / `gene_to_diseases` are the genuine ~100 M-edge tables — decide
   materialise vs counts-only vs keep-live once the codon run reports their true size.

---

## Implementation status

Both stages are implemented. `materialised_queries/` is gone; a template opts into
precomputation with a top-level `materialise:` block.

### Final YAML schema

```yaml
# --- always ---
title: ...
description: ...
graphs: [ebi_monarch_xspecies]     # (parameterised only; standalone uses run_for_subgraphs)
topics: [...]

# --- live parameterised (unchanged): fragments + params, served via Cypher ---
question: ...
cypher_match_fragment: |- ...
cypher_return_fragment: |- ...
cypher_count_fragment: |- ...
params: [...]
result_columns: [...]
examples: [...]

# --- standalone materialised (kind 2): a body with no params ---
materialise:
  cypher: |-  RETURN ...            # the query to run (was cypher_query)
  run_for_subgraphs: [impc_x_gwas]  # optional; absent => every subgraph
  uses_datasources: [IMPC, GWAS]    # display only

# --- materialised parameterised (kind 3): its own body doubles as the materialise query ---
materialise:
  mode: full                # full (store rows) | counts_only (store per-base-node counts)
  budget_rows: 15000000     # optional per-template row budget override
  params:
    - param_id: cell_type_id
      filters_column: cell_type   # the base result-column this param constrains at serving
      closure: descendants        # descendants | ancestors | exact (serving semantics)
      domain_kind: id             # id (CURIE root) | label (type label)
      domain_root: 'cl:0000000'   # domain root substituted when deriving the materialise query
```

**Derivation (no separate materialise Cypher).** At dataload the materialise query
is derived from the template's own match fragment by rewriting each param's Id
anchor so the base ranges over its whole domain
(`dataload/07_run_queries/grebi_materialise.py`):

- `domain_kind: id`, `closure: descendants|ancestors` — the body is authored in
  closure-root form (`(base)-[:broad_match*0..1]->(x)-[:sourceId]->(:Id {id: $p})`);
  substitute `$p` → the domain-root CURIE literal.
- `domain_kind: id`, `closure: exact` — the base is anchored directly; the anchor
  `-[:sourceId]->(:Id {id: $p})` is wrapped into
  `-[:broad_match*0..1]->(__p_dom)-[:sourceId]->(:Id {id: 'root'})`.
- `domain_kind: label` — drop the `-[:sourceId]->(:Id {id: $p})` anchor; the base
  keeps its type label (e.g. `(gene:\`hgnc:Gene\`)`).

Remaining non-closure params are substituted with their `param_default`. A build-size
budget gates the pipeline (fail unless `GREBI_MATERIALISE_BUDGET_OVERRIDE=true`).

### Serving (closure-at-query-time)

`GrebiPostgresClient.searchMaterialisedParameterised` resolves the queried value P
to the set of source CURIEs in its closure — P plus its `biolink:broad_match`
descendants (or ancestors, or just P for `exact`) via the precomputed closure in
`edges_{sg}` — then keeps the stored rows whose base column's `id` intersects that
set (`jsonb_exists_any(data -> col -> 'id', curies)`). Counts are `count(*)` over the
filtered rows (flat, cheap). `counts_only` stores a per-base `_count` histogram and
sums it over the closure (data served live). `GrebiApi.serveQueryTemplate` routes
`/query/{id}` and `.csv` to Postgres when a build exists in
`graph_metadata.materialised_templates`, else falls back to live Cypher.

### What is materialised

- **Standalone** (`materialise.cypher`, browsable `/tables`): `hello_world_tester`,
  `impc_x_gwas`.
- **Parameterised, full** (served from Postgres): the four rewritten roll-ups
  (`gwas_by_cell_type`, `gwas_by_location`, `gwas_by_disease_location`,
  `gwas_by_gene_and_cell_type`), `gwas_by_gene_and_disease`,
  `gwas_by_gene_and_location`, `gwas_traits_reported_different_from_matched`,
  `chebi_to_metabolights`, `phenotype_to_diseases`, and the five disease templates
  (`disease_to_{processes,exposures,treatments,locations,phenotypes}`).
- **Kept live** (documented, `materialise` absent): `disease_to_genes`,
  `gene_to_diseases` (the two ~125 M-edge tables — Q4 above, flip to `counts_only`
  or `full` once the codon size lands), `gwas_by_pathway` and
  `gwas_by_drugs_indicated_for_disease` (the base node isn't carried to the result
  and threading it through their `reactome:hasEvent*` / `CALL {}` subqueries needs a
  larger rewrite than the mechanical one), `gwas_trait_to_mouse_models_via_embeddings`
  (KNN special case) and `mouse_gene_to_opentargets` (a user-tunable `min_score`
  filter that materialisation would fix at its default).

The four Appendix B roll-ups were rewritten to the canonical base-keyed form (single
`biolink:broad_match` hop, `_root` enumeration dropped); the gwas gene templates were
given the behaviour-preserving `(gene:\`hgnc:Gene\`)` label.

### Testing

- `dataload/07_run_queries/test_grebi_materialise.py` — unit tests for the derivation
  transforms, plus a smoke pass over every real template (no unbound `$params`,
  valid `filters_column`).
- `webapp/.../MaterialisedClosureServingTest.java` — exercises the real
  `GrebiPostgresClient` closure serving against a live Postgres (descendants /
  ancestors / exact, counts_only, unknown-node, text filter, prefix stripping).
  Skipped unless `GREBI_TEST_POSTGRES=true`.
- `query_templates/test/test_ubergraph_subtypes.yaml` — a parameterised materialised
  template scoped to the `test_ubergraph` E2E subgraph (which has a real
  `biolink:broad_match` closure). It exercises the full Nextflow→Neo4j→Postgres path
  in CI: the pipeline derives its materialise query, runs it, stores the rows, and
  records the build in `graph_metadata.materialised_templates`. Its derived and live
  queries were verified against a Neo4j reconstructed from the committed
  `test_ubergraph` snapshot (root A → subtypes {A,B,C,D}; B → {B,C,D}). The
  `materialised_templates` metadata is a stable serving descriptor (id / mode /
  params, no run-dependent counts), so its api snapshot entry is deterministic.
- The E2E api snapshots were updated for the renamed standalone tester, the new
  `materialised_templates` key, and the test_ubergraph fixture entry.

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

## Appendix B: canonical base-keyed forms (the hard four)

These are the rewritten bodies. In each, the param's closure root is what varies:
bind it to the queried value for **live** serving, or to the `param_opts` domain
root (shown) for **materialise**. Validated as counts on the snapshot (drop the
`_root` enumeration; constrain the base term to its domain). See
the experiment at `/nfs/production/parkinso/spot/jmcl/grebi_query_timings` on codon
(`queries.jsonl` holds the exact bodies; `logs/results.tsv` the results). Reproduce
`RETURN count(*)` → materialise by swapping to the template's `RETURN DISTINCT ...`.

- `gwas_by_cell_type`: key on `cl_term`, `MATCH (cl_term)-[:biolink:broad_match*0..1]->(dr)-[:sourceId]->(:Id {id:'cl:0000000'})`, drop `cl_root`.
- `gwas_by_location`, `gwas_by_disease_location`: key on `location`, constrain to `uberon:0001062`, drop `location_root`.
- `gwas_by_gene_and_cell_type`: `(gene:\`hgnc:Gene\`)` + key on `cl_term` constrained to `cl:0000000`, drop `cl_root`.
