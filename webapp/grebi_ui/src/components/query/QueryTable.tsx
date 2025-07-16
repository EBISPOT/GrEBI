
import { useState, useEffect, Fragment, useMemo } from "react";
import GraphMetadata from "../../model/GraphMetadata"
import LocalDataTable from "../datatable/LocalDataTable"
import { get } from "../../app/api";
import { Box, Button, Chip, CircularProgress, Link, Stack } from "@mui/material";
import { Download, Info } from "@mui/icons-material";
import { useNavigate } from "react-router-dom";
import { QueryTemplate } from "../../model/QueryTemplate";
import QueryTopic from "../../model/QueryTopic";
import hardcodedNodeTypes from "../../hardcoded_node_types.json";
import NodeTypeChip from "../NodeTypeChip";
import * as Muicon from "@mui/icons-material";

function DynamicIcon({ iconName }: { iconName: string }) {
    const IconComponent = Muicon[iconName as keyof typeof Muicon];
    return IconComponent ? <IconComponent fontSize="small" /> : null;
}

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
                    // onSelectRow={(row) => {
                    //     navigate(`/subgraphs/${subgraph}/results/${row['id']}`)
                    // }}
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
            },
            sortable:true
        },
        {
            id:"title",
            name:"Name",
            selector:(row:any,key:string)=>{
                return addLinksToText(row[key], subgraph)
            },
            sortable:true
        },
        {
            id:"description",
            name:"Description",
            selector:(row:any,key:string)=>{
                return addLinksToText(row[key], subgraph)
            },
            sortable:true
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
                        return <Link key={e} href={`/subgraphs/${subgraph}/query/${row.id}`} underline="hover">
                            <Muicon.Search fontSize="small" sx={{ verticalAlign: 'middle' }} />
                            {e.title}
                        </Link>
                    })}
                </Stack>
            },
            sortable:true
        }
    ]
}

function addLinksToText(text:string, subgraph:string|undefined) {
    if(!subgraph)
        return text;

    let curieRegex = /\b([a-z]+:[a-z0-9_]+)\b/gi;

    return text.split(curieRegex).map((part, index) => {
        if(index % 2 === 0) {
            return part; // Non-CURIE part
        } else {
            // CURIE part
            let curie = part;
            for(let nodeType of hardcodedNodeTypes) {
                if(nodeType.types.indexOf(curie) !== -1) {
                    return <span
                className={`px-2 py-0.5 rounded-md text-xs uppercase font-bold ml-1`} style={{backgroundColor:nodeType.bgColor}} title={nodeType.longName}>

{ nodeType.icon &&
                    <DynamicIcon iconName={nodeType.icon} /> }
                    
                    {nodeType.longName}</span>
                }
            }
            return curie;
        }
    });
}