# test_pdbe data

A small **real** subset of PDBe SIFTS flat files, used by the `test_pdbe`
subgraph to exercise PDBe ingestion.

## Provenance

Extracted from the PDBe SIFTS TSV flat files on the EMBL-EBI FTP site
(release stamp `2026/06/21 | PDB: 25.26 | UniProt: 2026.03`):

- `pdb_chain_uniprot.tsv` — from
  <https://ftp.ebi.ac.uk/pub/databases/msd/sifts/flatfiles/tsv/pdb_chain_uniprot.tsv.gz>
- `pdb_chain_enzyme.tsv` — from
  <https://ftp.ebi.ac.uk/pub/databases/msd/sifts/flatfiles/tsv/pdb_chain_enzyme.tsv.gz>

Only the rows for a curated set of well-known PDB entries were kept (the original
two-line header — date comment + column header — is preserved verbatim):

| PDB | Description | Notes exercised |
| --- | --- | --- |
| `101m`, `102m` | Myoglobin (P02185) | multiple EC numbers per structure |
| `102l`, `103l`, `104l`, `107l` | T4 lysozyme (P00720) | duplicate/segmented rows merge into one node; EC 3.2.1.17 |
| `1cbs` | Cellular retinoic-acid-binding protein (P29373) | single chain, no EC |
| `1hho`, `4hhb` | Haemoglobin | multi-chain complex → two distinct UniProt proteins (P69905 α, P68871 β) |
| `2lyz` | Hen egg-white lysozyme (P00698) | EC 3.2.1.17 |
| `5cmd` | (P13501) | chains with empty coordinate fields |

`reference_nodes.jsonl` is a small companion set of the **real** UniProt protein
and EC enzyme entities that these structures reference (ingested as the
`ProteinsAndEnzymes` test datasource). In the full EBI graph these entities come
from the UniProt/enzyme datasources; the stand-in lets the structure→protein and
structure→enzyme edges actually form in this isolated subgraph (GrEBI only
creates an edge when the referenced entity is present in the graph).

## What it demonstrates

- **Merge**: each PDB entry spans several SIFTS rows (one per chain/segment) and
  appears in both files; all rows for a PDB id collapse into a single
  `pdb:Structure` node (e.g. `pdb:4hhb` → 4 chains, both haemoglobin subunits).
- **Edge linking**: `SP_PRIMARY` becomes a `uniprot:` CURIE that links the
  structure to its UniProt protein(s) — turning the `pdb:` references that
  already arrive from UniProt/Reactome/etc. into first-class merged entities.
- **Ontology mapping**: `EC_NUMBER` becomes an `ec:` CURIE, linking structures
  onto enzyme classification (a vocabulary GrEBI already loads).

## Regenerating / extending

To refresh or add entries, download the full `.gz` files from the URLs above,
keep the first two lines, and append the rows you want (`grep`-ed on the first
column). The production datasource that ingests the full files directly from
the FTP site is `configs/datasource_configs/pdbe.yaml`.
