import { useParams, useSearchParams } from "react-router-dom";
import MaterialisedQueryTable from "../../../components/matq/MaterialisedQueryTable";
import EbiHeader from "../EbiHeader";
import { useState } from "react";
import { Link, Typography } from "@mui/material";
import ResultsTable from "../../../components/matq/ResultsTable";

export default function EbiTablesPage() {

  let params = useParams();
  let [searchParams, setSearchParams] = useSearchParams();
  let subgraph:string|undefined = params.subgraph
  let queryid:string|undefined = params.queryid

  if(!subgraph || !queryid) {
    throw new Error("??");
  }

    return (
        <div>
        <EbiHeader subgraph={subgraph} section="tables" showBreadcrumbsBar={true} breadcrumbs={[
          { url: `/subgraphs/${subgraph}/tables`, label: "Tables" },
          { url: `/subgraphs/${subgraph}/tables/${queryid}`, label: <code>{queryid}</code> }
        ]} />
        <main className="container mx-auto px-4 h-fit pt-2">
        <div className="grid grid-cols-2 lg:grid-cols-1 lg:gap-8">
            <Typography variant="h4">{queryid}</Typography>
            <ResultsTable subgraph={subgraph} queryid={queryid} />
        </div>
        </main>
        </div>
    );
}

