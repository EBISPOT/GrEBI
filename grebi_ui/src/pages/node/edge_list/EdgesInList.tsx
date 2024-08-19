
import { Grid } from "@mui/material";
import GraphNode from "../../../model/GraphNode";
import React, { Fragment, useEffect } from "react";
import { get } from "../../../app/api";

export default function EdgesInList(params:{
    subgraph:string,
    node:GraphNode,
    datasources:string[],
    dsEnabled:string[]
}) {
    let { subgraph, node, datasources, dsEnabled } = params

    let [edges, setEdges] = React.useState([])

    useEffect(() => {
        async function getEdges() {
            let edges = await get<any>(`api/v1/subgraphs/${subgraph}/nodes/${node.getNodeId()}/incoming_edges`)
            setEdges(edges)
        }
        getEdges()

    }, [node.getNodeId()]);
        
    
    return <div/>


}
