import { useLocation, useParams, useSearchParams } from "react-router-dom";
import MaterialisedQueryTable from "../../../components/matq/MaterialisedQueryTable";
import EbiBreadcrumbsBar from "../EbiBreadcrumbsBar";
import { useEffect, useState } from "react";
import { Link, Typography } from "@mui/material";
import ResultsTable from "../../../components/matq/ResultsTable";
import { QueryTemplate } from "../../../model/QueryTemplate";
import {get} from "../../../app/api";
import addLinksToText from "../../../addLinksToText";
import QueryInterface from "../../../components/query/QueryInterface";
import QueryQuestion from "../../../components/query/QueryQuestion";
import InputBadge from "../../../components/query/InputBadge";
import OutputBadge from "../../../components/query/OutputBadge";

export default function EbiQueriesPage() {

  let params = useParams();
  let [searchParams, setSearchParams] = useSearchParams();
  let graph:string|undefined = params.graph
  let queryid:string|undefined = params.queryid

  let [queryTemplate, setQueryTemplate] = useState<QueryTemplate|undefined>(undefined)

    useEffect(() => {
        get<QueryTemplate>(`api/v1/graphs/${graph}/query_templates/${queryid}`)
            .then(r => setQueryTemplate(r));
    }, [graph, queryid]);

  if(!graph || !queryid) {
    throw new Error("??");
  }

  let breadcrumbs = [
    { url: `/graphs`, label: "Graphs" },
    { url: `/graphs/${graph}/queries`, label: "Queries" },
    { url: `/graphs/${graph}/queries/${queryid}`, label: <code>{queryid}</code> }
  ]

    return (
        <div>
        <EbiBreadcrumbsBar graph={graph} entries={breadcrumbs} />
        <main className="container mx-auto px-4 h-fit pt-2">

          { !queryTemplate && 
            <div className="spinner-default w-7 h-7" />
          }

          { queryTemplate &&
            <div>
              <Typography variant="h4" sx={{ mt: 3 }}>{addLinksToText(queryTemplate.title, graph)}</Typography>
              <p className="text-lg text-neutral-dark mt-6 mb-6">
                  {queryTemplate.question.split(/(\[[^\]]+\]\{[^}]+\}|\{[^}]+\})/).map((part, i) => {
                    let mRef = part.match(/^\[([^\]]+)\]\{(.+)\}$/);
                    if (mRef) {
                      return <span key={i}><strong>{mRef[1]}</strong> (<OutputBadge>{mRef[2]}</OutputBadge>)</span>;
                    }
                    let mParam = part.match(/^\{(.+)\}$/);
                    if (mParam) {
                      return <InputBadge key={i}>{mParam[1]}</InputBadge>;
                    }
                    return part;
                  })}
              </p>
              <QueryInterface graph={graph} queryTemplate={queryTemplate} sidebar={
                queryTemplate.examples && queryTemplate.examples.length > 0 ? (
                  <div>
                    <Typography variant="subtitle2" className="text-gray-500 mb-2">Examples</Typography>
                    <div className="flex flex-col gap-2 mt-2">
                      {queryTemplate.examples.map((example, idx) => (
                        <div
                          key={idx}
                          className="text-left text-sm hover:bg-blue-50 rounded px-2 py-1.5 transition-colors cursor-pointer"
                          onClick={() => {
                            const qs = new URLSearchParams(example.params);
                            setSearchParams(qs);
                          }}
                        >
                          <QueryQuestion graph={graph} template={queryTemplate} exampleIndex={idx} readOnly={true} fontSize="0.85rem" />
                        </div>
                      ))}
                    </div>
                  </div>
                ) : undefined
              } />
            </div>
          }


        </main>
        </div>
    );
}

