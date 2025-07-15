import { useParams, useSearchParams } from "react-router-dom";
import MaterialisedQueryTable from "../../../components/matq/MaterialisedQueryTable";
import EbiHeader from "../EbiHeader";
import { useState } from "react";
import { Link, Typography } from "@mui/material";

export default function EbiTablesHomePage() {

  let params = useParams();
  let [searchParams, setSearchParams] = useSearchParams();
  let subgraph:string|undefined = params.subgraph
  let queryid:string|undefined = params.queryid
  
    return (
        <div>
        <EbiHeader section="tables" />
        <main className="container mx-auto px-4 h-fit pt-2">
        <div className="grid grid-cols-2 lg:grid-cols-1 lg:gap-8">
            <Typography variant="h4">Materialised Result Tables</Typography>

        <p>Querying knowledge graphs at scale can be prohibitively computationally expensive without access to HPC. GrEBI therefore materialises the complete results of a pre-defined collection of Cypher queries, defined by YAML files in the <Link className="link-default" href="https://github.com/EBISPOT/GrEBI/tree/dev/materialised_queries/">GrEBI GitHub repository</Link>. </p>
                <p>
             The materialised result tables are searchable and browseable via this website and API, and can also be downloaded in CSV format, enabling data integration outputs of the knowledge graph to be used without any specialised tooling or compute.
            </p>
            <MaterialisedQueryTable subgraph={subgraph} />
        </div>
        </main>
        </div>
    );
}
