/**
 * Where releases are published. dataload/scripts/ebi_datarelease_to_ftp.sh
 * copies a release into a dated folder and into latest/: per graph a Neo4j
 * archive and a metadata file, the whole release as one archive (the
 * standalone Postgres archive only when the dataload packages it), and the
 * materialised tables of each graph as gzipped CSV and as Parquet.
 */
export const FTP_BASE = (process.env.REACT_APP_FTP_BASE || "https://ftp.ebi.ac.uk/pub/databases/spot/kg").replace(/\/+$/, "");
export const LATEST_RELEASE = `${FTP_BASE}/latest`;

export interface ReleaseFile {
  description: string;
  file: string;
  url: string;
  format: string;
}

/** The files of the latest release that concern one graph. */
export function releaseFiles(graph: string): ReleaseFile[] {
  return [
    {
      description: `Neo4j database of ${graph}`,
      file: `${graph}_neo4j.tar.xz`,
      url: `${LATEST_RELEASE}/${graph}_neo4j.tar.xz`,
      format: "Neo4j database, tar.xz",
    },
    {
      description: `Metadata of ${graph}: its datasources, node and edge types, property counts and materialised queries`,
      file: `${graph}_metadata.json`,
      url: `${LATEST_RELEASE}/${graph}_metadata.json`,
      format: "JSON",
    },
    {
      description: `Materialised tables of ${graph}: the whole results of its queries, each as gzipped CSV and as Parquet`,
      file: `query_results/${graph}/`,
      url: `${LATEST_RELEASE}/query_results/${graph}/`,
      format: "Directory of CSV (gzipped) and Parquet",
    },
    {
      description: "PostgreSQL database of every graph in the release: all properties of all nodes and edges, the search indexes and the materialised tables (published when the dataload packages it separately)",
      file: "postgres.tar.xz",
      url: `${LATEST_RELEASE}/postgres.tar.xz`,
      format: "PostgreSQL data directory, tar.xz",
    },
    {
      description: "The whole release: every database, the metadata, the query templates and a script that runs it with the GrEBI image",
      file: "release.tar.xz",
      url: `${LATEST_RELEASE}/release.tar.xz`,
      format: "tar.xz",
    },
  ];
}

export type TableFormat = "csv" | "parquet";

/**
 * A materialised table of a graph in the latest release. Two graphs can
 * materialise the same query, so a table is a file of its graph's folder.
 */
export function tableFileUrl(graph: string, tableId: string, format: TableFormat): string {
  return `${LATEST_RELEASE}/query_results/${graph}/${tableId}.${format === "csv" ? "csv.gz" : "parquet"}`;
}
