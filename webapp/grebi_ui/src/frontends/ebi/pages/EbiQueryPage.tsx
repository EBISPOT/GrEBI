import { useLocation, useParams, useSearchParams } from "react-router-dom";
import MaterialisedQueryTable from "../../../components/matq/MaterialisedQueryTable";
import EbiHeader from "../EbiHeader";
import { useEffect, useState } from "react";
import { Link, Typography } from "@mui/material";
import ResultsTable from "../../../components/matq/ResultsTable";
import { QueryTemplate } from "../../../model/QueryTemplate";
import {get} from "../../../app/api";
import addLinksToText from "../../../addLinksToText";
import QueryInterface from "../../../components/query/QueryInterface";

export default function EbiQueriesPage() {

  let params = useParams();
  let [searchParams, setSearchParams] = useSearchParams();
  let subgraph:string|undefined = params.subgraph
  let queryid:string|undefined = params.queryid

  let [queryTemplate, setQueryTemplate] = useState<QueryTemplate|undefined>(undefined)

    useEffect(() => {
        get<QueryTemplate>(`api/v1/subgraphs/${subgraph}/query_templates/${queryid}`)
            .then(r => setQueryTemplate(r));
    }, [subgraph, queryid]);

  if(!subgraph || !queryid) {
    throw new Error("??");
  }

  let breadcrumbs = [
    { url: `/subgraphs/${subgraph}/queries`, label: "Queries" },
    { url: `/subgraphs/${subgraph}/queries/${queryid}`, label: <code>{queryid}</code> }
  ]

    return (
        <div>
        <EbiHeader section="queries" subgraph={subgraph} showBreadcrumbsBar={true} breadcrumbs={breadcrumbs} />
        <main className="container mx-auto px-4 h-fit pt-2">

          { !queryTemplate && 
            <div className="spinner-default w-7 h-7" />
          }

          { queryTemplate &&
            <div className="grid grid-cols-2 lg:grid-cols-1 lg:gap-8">
              <Typography variant="h4">{addLinksToText(queryTemplate.title, subgraph)}</Typography>
              <p>{addLinksToText(queryTemplate.description, subgraph)}</p>
              <div className="grid grid-cols-2 lg:grid-cols-1 lg:gap-2">
              <QueryInterface subgraph={subgraph} queryTemplate={queryTemplate} />
              </div>
            </div>
          }


        </main>
        </div>
    );
}

