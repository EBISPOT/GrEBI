import { useParams, useSearchParams } from "react-router-dom";
import MaterialisedQueryTable from "../../../components/matq/MaterialisedQueryTable";
import EbiBreadcrumbsBar from "../EbiBreadcrumbsBar";
import { useState } from "react";
import { Link, Typography } from "@mui/material";
import ResultsTable from "../../../components/matq/ResultsTable";

export default function EbiTablesPage() {

  let params = useParams();
  let [searchParams, setSearchParams] = useSearchParams();
  let graph:string|undefined = params.graph
  let queryid:string|undefined = params.queryid

  if(!graph || !queryid) {
    throw new Error("??");
  }

    return (
        <div>
        <EbiBreadcrumbsBar graph={graph} entries={[
          { url: `/graphs`, label: "Graphs" },
          { url: `/graphs/${graph}/tables`, label: "Tables" },
          { url: `/graphs/${graph}/tables/${queryid}`, label: <code>{queryid}</code> }
        ]} />
        <main className="container mx-auto px-4 h-fit pt-2">
        <div className="grid grid-cols-2 lg:grid-cols-1 lg:gap-8">
            <Typography variant="h4">{queryid}</Typography>
            <ResultsTable graph={graph} queryid={queryid} />
        </div>
        </main>
        </div>
    );
}

