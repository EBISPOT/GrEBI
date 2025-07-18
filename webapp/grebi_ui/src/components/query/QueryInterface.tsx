

import { useEffect, useState } from "react";
import { QueryTemplate } from "../../model/QueryTemplate";
import QueryTopic from "../../model/QueryTopic";
import {get, getPaginated} from "../../app/api";
import NodeSelectorBox from "../NodeSelectorBox";
import { Box, Typography, FormControl, TextField, InputLabel, Button, Table, TableBody, TableCell, TableRow, Stack } from "@mui/material";
import { useSearchParams } from "react-router-dom";
import GraphNodeRef from "../../model/GraphNodeRef";
import { Search } from "@mui/icons-material";
import ResultsTable from "./ResultsTable";
import { Link } from "react-router-dom";

export default function QueryInterface({
    subgraph,
    queryTemplate
}:{
    subgraph:string, queryTemplate:QueryTemplate
}) {

    let params = queryTemplate.params

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
                            let nodeDetails = new GraphNodeRef( (await getPaginated<any>(`api/v1/subgraphs/${subgraph}/nodes`, { "grebi:sourceIds": nodeId, resolve: "false" })).elements[0]);
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

return (
  <Box sx={{ p: 3 }}>
    <Box
      component="form"
    >
        <Typography variant="h5" gutterBottom>
        Examples
        </Typography>

        <Box sx={{ mb: 3 }}>
            {queryTemplate.examples && queryTemplate.examples.length > 0 ? (
                <Stack direction="column" spacing={0}>
                    {queryTemplate.examples.map((example, index) => {
                        let exampleParams = new URLSearchParams(example.params).toString();
                        return (
                            <Link key={index} to={`/subgraphs/${subgraph}/queries/${queryTemplate.id}?${exampleParams}`} className="link-default">
                                <Box sx={{ display: 'flex', alignItems: 'center' }}>
                                    <Search fontSize="small" sx={{ verticalAlign: 'middle', mr: 1 }} />
                                    {example.title}
                                </Box>
                            </Link>
                        );
                    })}
                </Stack>
            ) : (
                <Typography variant="body2" color="textSecondary">
                No examples available for this query.
                </Typography>
            )}
        </Box>

        <Typography variant="h5" gutterBottom>
        Parameters
        </Typography>
      <Table sx={{ mb: 3 }}>
        <TableBody>
          {params.map((param) => (
            <TableRow key={param.param_id}>
              <TableCell
                sx={{ width: "200px", verticalAlign: "middle", fontSize:"large" }}
              >
                {param.param_name}
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
                    subgraph={subgraph}
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
    </Box>

    {paramValuesSubmitted && <ResultsTable subgraph={subgraph} queryId={queryTemplate.id} params={paramValuesSubmitted} resultColumns={queryTemplate.result_columns} />}
  </Box>
);
}



