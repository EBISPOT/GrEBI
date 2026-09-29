import { useParams } from "react-router-dom";
import MaterialisedQueryTable from "../../../components/matq/MaterialisedQueryTable";
import EbiBreadcrumbsBar from "../EbiBreadcrumbsBar";
import { Typography } from "@mui/material";
import { LATEST_RELEASE, tableFileUrl } from "../../../app/ftp";

export default function EbiTablesHomePage() {

  let params = useParams();
  let graph:string|undefined = params.graph

  if(!graph) {
    throw new Error("??");
  }
  
    return (
        <div>
        <EbiBreadcrumbsBar graph={graph} entries={[
          { url: `/graphs`, label: "Graphs" },
          { url: `/graphs/${graph}/tables`, label: "Tables" }
        ]} />
        <main className="container mx-auto px-4 h-fit pt-2">
        <div className="grid grid-cols-2 lg:grid-cols-1 lg:gap-8">
            <Typography variant="h4">Materialised Result Tables</Typography>
            <p>
                When the knowledge graph is built, each of its queries is also run whole, for everything its
                parameters can take. The resulting tables are published with the release, at&thinsp;
                <a className="link-default" href={`${LATEST_RELEASE}/query_results/${graph}/`} rel="noopener noreferrer" target="_blank">query_results/{graph}/</a>,
                as gzipped CSV and as Parquet. A node is two columns, its identifier and its label.
            </p>
            <p>
                The Parquet files are typed and can be queried where they are, without downloading them, for
                example with DuckDB:
            </p>
            <pre className="text-sm bg-gray-50 border border-gray-200 rounded p-3 overflow-x-auto">
{`SELECT * FROM '${tableFileUrl(graph, "disease_to_genes", "parquet")}'
WHERE disease_id = 'mondo:0004975';`}
            </pre>
            <MaterialisedQueryTable graph={graph} />
        </div>
        </main>
        </div>
    );
}
