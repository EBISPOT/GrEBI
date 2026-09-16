# test_gwas data

A small **real** subset of the NHGRI-EBI GWAS Catalog, used by the `test_gwas`
subgraph to exercise `grebi_ingest_gwas` on the column formats the catalog uses
to pack several values into one cell (GitHub issue #12).

## Provenance

Rows copied verbatim (header line preserved) from the 2026-09-15 catalog release:

- `gwas-catalog-download-associations-alt-full.tsv` — from
  <https://ftp.ebi.ac.uk/pub/databases/gwas/releases/latest/gwas-catalog-associations_ontology-annotated-full.zip>
- `gwas-catalog-studies-download-alternative-v1.0.2.1.txt` — from
  <https://ftp.ebi.ac.uk/pub/databases/gwas/releases/latest/gwas-catalog-studies-download-alternative-v1.0.2.1.txt>
  (the seven studies the association rows cite)

The filenames matter: the ingester dispatches on `associations` vs `studies` in
the path, and the production datasource (`configs/datasource_configs/gwas.yaml`)
ingests files of exactly these names.

## What each association row exercises

| SNPS | MAPPED_GENE | Exercises |
| --- | --- | --- |
| `rs2517582` | `LINC02570` | plain single gene, linked via `SNP_GENE_IDS` |
| `rs1245314` | `NOVA1-DT, MIR4307HG` | `, ` list: variant within two overlapping genes; hyphen inside a symbol |
| `rs162212` | `GRM7 - LMCD1-AS1` | ` - ` intergenic pair, linked via `UPSTREAM_GENE_ID`/`DOWNSTREAM_GENE_ID`; two mapped traits |
| `rs3130453 x rs2249742` | `CCHCR1 x HLA-C - USP8P1` | ` x ` SNP-SNP interaction nesting an intergenic pair; no Ensembl ids |
| `SNP_A-2032347; rs10871290; SNP_A-2196879` | `No mapped genes; CLEC18B - GLG1; No mapped genes` | `; ` multi-SNP haplotype with the `No mapped genes` placeholder |
| `rs201450565` | `EEFSEC` | two traits whose labels split cleanly (`premature birth, parental genotype effect measurement`) |
| `rs4733724` | `CCDC26` | one trait whose label itself contains `, ` (`osteoarthritis, hip`) |
| `rs2622873` | `COL11A1` | two traits whose labels both contain `, ` (label/URI counts disagree, so the whole label string is kept) |

`reference_nodes.jsonl` (the eight HGNC genes the Ensembl ids refer to, looked
up from <https://rest.genenames.org/>) and `reference_traits.jsonl` (the nine
EFO/MONDO traits) are minimal stand-ins for the HGNC and OLS datasources, so the
variant->gene, variant->trait and study->trait edges actually form in this
isolated subgraph.

## Regenerating / extending

Download the two files from the URLs above, keep the header line, and append
the rows you want (`grep` on the SNPS column). Add any newly referenced genes
and traits to the reference files.
