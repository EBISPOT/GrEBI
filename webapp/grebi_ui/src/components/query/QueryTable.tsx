
import { useState, useEffect, Fragment, useMemo } from "react";
import GraphMetadata from "../../model/GraphMetadata"
import LocalDataTable from "../datatable/LocalDataTable"
import { get } from "../../app/api";
import { Box, Button, Chip, CircularProgress, Stack } from "@mui/material";
import { Download, Info } from "@mui/icons-material";
import { useNavigate } from "react-router-dom";
import { QueryTemplate } from "../../model/QueryTemplate";
import QueryTopic from "../../model/QueryTopic";
import hardcodedNodeTypes from "../../hardcoded_node_types.json";
import NodeTypeChip from "../NodeTypeChip";
import addLinksToText from "../../addLinksToText";
import * as Muicon from "@mui/icons-material";
import { Link } from "react-router-dom";

export default function QueryTable({
    subgraph
}:{
    subgraph?:string|undefined
}) {


  let [topics, setTopics] = useState<QueryTopic[]|null>(null);
  let [queries, setQueries] = useState<QueryTemplate[]|null>(null);

  const navigate = useNavigate();

    useEffect(() => {
        get<QueryTemplate[]>(`api/v1/subgraphs/${subgraph}/query_templates`).then(r => setQueries(r));
    }, [subgraph])

    useEffect(() => {
        get<QueryTopic[]>(`api/v1/topics`).then(r => setTopics(r));
    }, [])

    let cols = useMemo(() => {
        return getColumns(subgraph, topics)
    }, [subgraph, topics]);

    if(!queries || !topics) {
        return <CircularProgress />
    }


    return <LocalDataTable
                    data={queries} 
                    addColumnsFromData={false}
                    defaultSelector={(row,key)=>row[key]}
                    columns={cols}
                    onSelectRow={(row) => {
                        navigate(`/subgraphs/${subgraph}/queries/${row['id']}`)
                    }}
                    />

}

function getColumns(subgraph:string|undefined, topics:QueryTopic[]|null) {
    if(!subgraph || !topics)
        return undefined
    return [
        {
            id:"topics",
            name:"Topics",
            selector:(row:any,key:string)=>{
                let queryTopics = row['topics'];
                if(!queryTopics || queryTopics.length === 0) {
                    return <Fragment/>
                }
                return <Stack direction="row" spacing={1}>
                    {queryTopics.map((t:any) => {
                        let topic = topics?.find((topic) => topic.id === t)!;
                        return <Chip key={topic.id} label={topic.name} />
                    })}
                </Stack>
            }
        },
        {
            id:"title",
            name:"Name",
            selector:(row:any,key:string)=>{
                return addLinksToText(row[key], subgraph)
            },
            className: "group-hover:text-blue-600 group-hover:underline"
        },
        {
            id:"description",
            name:"Description",
            selector:(row:any,key:string)=>{
                return addLinksToText(row[key], subgraph)
            }
        },
        {
            id:"examples",
            name:"Examples",
            selector:(row:any,key:string)=>{
                let examples = row['examples']
                if(!examples || examples.length === 0) {
                    return <Fragment/>
                }
                return <Stack direction="column">
                    {examples.map((e:any) => {
                        // encode example params as a query string
                        let exampleParams = new URLSearchParams(e.params).toString();
                        return <Link key={e} className="link-default" to={`/subgraphs/${subgraph}/queries/${row.id}?${exampleParams}`}>
                            <Muicon.Search fontSize="small" sx={{ verticalAlign: 'middle' }} />
                            {e.title}
                        </Link>
                    })}
                </Stack>
            }
        }
    ]
}
