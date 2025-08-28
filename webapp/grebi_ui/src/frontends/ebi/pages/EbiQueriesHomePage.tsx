import { useParams, useSearchParams } from "react-router-dom";
import MaterialisedQueryTable from "../../../components/matq/MaterialisedQueryTable";
import EbiHeader from "../EbiHeader";
import { useState } from "react";
import { Link, Typography } from "@mui/material";
import QueryTable from "../../../components/query/QueryTable";

export default function EbiTablesHomePage() {

  let params = useParams();
  let [searchParams, setSearchParams] = useSearchParams();
  let subgraph:string|undefined = params.subgraph
  let queryid:string|undefined = params.queryid

  if(!subgraph) {
    throw new Error("Subgraph is required");
  }
  
    return (
        <div>
        <EbiHeader
            section="queries"
            subgraph={subgraph}
            showBreadcrumbsBar={true}
            breadcrumbs={[
              { url: `/subgraphs/${subgraph}/queries`, label: "Queries" }
            ]}
        />
        <main className="container mx-auto px-4 h-fit pt-2">
        <div className="grid grid-cols-2 lg:grid-cols-1 lg:gap-8">
            <Typography variant="h4">Graph Queries</Typography>
            <p>
            The EBI KG can be queried using a selection of graph query templates, for which you can fill in your own parameters. Results are returned in a tabular format and can be downloaded as CSV. These query templates are also available as tools on the MCP server.</p>
            <QueryTable subgraph={subgraph} />
        </div>
        </main>
        </div>
    );
}
