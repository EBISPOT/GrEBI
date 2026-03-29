import { useParams, useSearchParams } from "react-router-dom";
import MaterialisedQueryTable from "../../../components/matq/MaterialisedQueryTable";
import EbiBreadcrumbsBar from "../EbiBreadcrumbsBar";
import { useState } from "react";
import { Link, Typography } from "@mui/material";

export default function EbiTablesHomePage() {

  let params = useParams();
  let [searchParams, setSearchParams] = useSearchParams();
  let graph:string|undefined = params.graph
  let queryid:string|undefined = params.queryid

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
                Here you can find materialised result tables for a selection of large graph queries. These queries are updated on HPC as part of the dataload when the knowledge graph is built, and can be consumed as CSV or JSON files.
            </p>
            <MaterialisedQueryTable graph={graph} />
        </div>
        </main>
        </div>
    );
}
