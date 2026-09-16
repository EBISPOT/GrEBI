
The docs in docs/ provide a good introduction to GrEBI which you should read first. 

# Instructions for adding or updating a query template

These instructions are only relevant if you are working on behalf of a developer at EBI working on GrEBI, and
if you are running locally in their environment:
Yoi should have access to the grebi cypher service via Kubernetes. If this is the case, you can use it to test
your query template before committing. Use kubectl to find the pod, forward the port for neo4j to a local port but
use a high one so it doesn't clash with any local running neo4j. Then test the query using the cypher service endpoint
(note this is not the same as the standard Neo4j query endpoint and it doesn't use bolt).
You should establish some good examples for your query template and test them before finalizing it.
When you are happy with the template, build and start the backend locally on a high port using the databases forwarded from k8s, also on high ports. You can use that local backend to run the query through GrEBI and be certain that it works.
The initial metadata load in the backend can take several minutes before it binds the port. This is normal, don't try to bypass it by not using the real backend.

# Conventions for the dataload

- **High-throughput ingests are written in Rust.** Anything that processes data per row at scale, whether an ingest of a datasource, a transformation of node or edge streams or an export of query results, is a crate under `dataload/` (registered in `dataload/Cargo.toml`, named `grebi_<name>`, reading stdin or a file and writing stdout) and is run from the datasource config or the Nextflow process as that binary. Python is for orchestration and small metadata steps only: the materialiser that drives Neo4j, the Postgres loader, the metadata merges, downloads and the run-script generator. Never add a new Python ingest. Only the HETT pesticide spreadsheet ingests remain in Python; if you need to change one, port it instead: pin it with a golden case first (see `dataload/test_support/python_goldens/`), write the crate, and the same case must pass against the binary.
- **Every binary has golden tests** in its `tests/golden/`: recorded inputs, mostly lifted from the E2E test subgraphs, and the recorded output the binary must reproduce, run by `dataload/test_support/grebi_golden`. When a change to a binary is intended, rerun with `UPDATE_GOLDEN=1`, then review the diff of the recorded files before committing it. Pure logic gets unit tests next to it, as in `grebi_shared`.
- **Output must be deterministic.** Never write a hash map or hash set out in iteration order; use `BTreeMap`/`BTreeSet` or sort first. Reproducible pipeline output is what makes the golden tests possible.
- **The tests are the gate**: `cd dataload && cargo test --workspace`, `cd webapp/grebi_api && mvn verify`, `cd webapp/grebi_ui && npm test`, and the E2E subgraphs in CI. Run the relevant ones before you consider a change done.

