

import { Fragment, useEffect, useMemo, useState } from "react";
import { QueryTemplate } from "../../model/QueryTemplate";
import QueryTopic from "../../model/QueryTopic";
import {get, getPaginated} from "../../app/api";
import NodeSelectorBox from "../NodeSelectorBox";
import { Box, Typography, FormControl, TextField, InputLabel, Button, Table, TableBody, TableCell, TableRow } from "@mui/material";
import { useSearchParams } from "react-router-dom";
import GraphNodeRef from "../../model/GraphNodeRef";
import InputBadge from "./InputBadge";
import ResultsTable from "./ResultsTable";
import TabbedSourceView from "./TabbedSourceView";
import query2code from "./query2code";
import React from "react";


export default function QueryInterface({
    graph,
    queryTemplate,
    sidebar
}:{
    graph:string, queryTemplate:QueryTemplate, sidebar?:React.ReactNode
}) {

    let params = queryTemplate.params || []

    let [queryParams, setQueryParams] = useSearchParams();

    let [paramValues, setParamValues] = useState<Record<string, any>>({});
    let [paramValuesSubmitted, setParamValuesSubmitted] = useState<Record<string, any>|undefined>(undefined);

    useEffect(() => {

        async function setParamsFromQueryString() {
            let initialValues: Record<string, any> = {};
            for(let param of params) {
                if (queryParams.has(param.param_id)) {
                    let valueFromQs = queryParams.get(param.param_id);

                    if (param.param_type === "SourceId") {

                        // If the parameter is a SourceId, we need to fetch the node details to display the name and type in the node selector box
                        let nodeId = queryParams.get(param.param_id);
                        if (nodeId) {
                            // Fetch the node details using the nodeId
                            let nodeDetails = new GraphNodeRef( (await getPaginated<any>(`api/v1/graphs/${graph}/nodes`, { "grebi:sourceIds": nodeId, resolve: "false" })).elements[0]);
                            initialValues[param.param_id] = nodeDetails
                        }

                    } else {
                        initialValues[param.param_id] = valueFromQs;
                    }

                } else {
                    initialValues[param.param_id] = param.param_default; // maybe undefined
                }
            }
            setParamValues(initialValues);
        }

        setParamsFromQueryString();

    }, [params, queryParams]);

    useEffect(() => {
        submit()
    }, [paramValues]);

    function submit() {

        const valuesToSend: Record<string, any> = {};
        for (let param of params) {
          if(paramValues[param.param_id] === undefined) {
                // incomplete params, don't submit
                return;
           }

          if (param.param_type === "SourceId") {            
            valuesToSend[param.param_id] = paramValues[param.param_id].getId().value;
          } else {
            valuesToSend[param.param_id] = paramValues[param.param_id];
          }
        }

        console.log("Submitting values:", valuesToSend);

        setParamValuesSubmitted(valuesToSend);
    }

 let cypherSource = (queryTemplate.cypher_match_fragment || "").trim() + "\n" + (queryTemplate.cypher_return_fragment || "").trim();
 let codeSnippets = query2code(queryTemplate, graph, paramValues)

 const sourceTabs = [
   ...Object.keys(codeSnippets).map(key => ({ title: key, source: codeSnippets[key].source, lang: codeSnippets[key].lang })),
   { title: "Cypher Query", source: cypherSource, lang: "cypher" },
 ];

return (
  <Fragment>

    <div className="flex gap-4 items-stretch mb-4">
      <div className="flex-1 min-w-0 flex flex-col">
        <TabbedSourceView tabs={sourceTabs} />
      </div>
      {sidebar && (
        <div className="flex-shrink-0 bg-gray-50 border border-gray-200 rounded-lg p-4 overflow-y-auto" style={{ width: 350 }}>
          {sidebar}
        </div>
      )}
    </div>

    <Box
      component="form"
    >

        {params && params.length > 0 &&
        <Fragment>
        <Typography variant="h5" gutterBottom>
        Inputs
        </Typography>
      <Table sx={{ mb: 3, '& tr:last-child td': { borderBottom: 'none' } }}>
        <TableBody>
          {params.map((param) => (
            <TableRow key={param.param_id}>
              <TableCell
                sx={{ width: "200px", verticalAlign: "middle", fontSize:"large" }}
              >
                <InputBadge>{param.param_id}</InputBadge>
              </TableCell>
              <TableCell>
                {param.param_type === "string" && (
                  <TextField
                    fullWidth
                    variant="outlined"
                    size="small"
                    value={paramValues[param.param_id] || undefined}
                    onChange={(e) =>
                      setParamValues({
                        ...paramValues,
                        [param.param_id]: e.target.value,
                      })
                    }
                  />
                )}

                {param.param_type === "float" && (
                  <TextField
                    fullWidth
                    variant="outlined"
                    size="small"
                    type="number"
                    value={paramValues[param.param_id] || undefined}
                    onChange={(e) =>
                      setParamValues({
                        ...paramValues,
                        [param.param_id]: e.target.value,
                      })
                    }
                  />
                )}
                {param.param_type === "SourceId" && (
                  <NodeSelectorBox
                    graph={graph}
                    selectedNode={paramValues[param.param_id]}
                    onNodeSelect={(node) =>
                      setParamValues({
                        ...paramValues,
                        [param.param_id]: node,
                      })
                    }
                    onClear={() => {
                        setParamValues({
                            ...paramValues,
                            [param.param_id]: undefined,
                        });
                    }}
                    additionalParams={param.param_opts ? new URLSearchParams(param.param_opts) : undefined}
                  />
                )}
              </TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>
        </Fragment>
}
    </Box>

    {paramValuesSubmitted !== undefined && <Fragment>
      <Typography variant="h5" gutterBottom>Results</Typography>
      <ResultsTable graph={graph} queryId={queryTemplate.id} params={paramValuesSubmitted} resultColumns={queryTemplate.result_columns} />
    </Fragment>}
  </Fragment>
);
}



