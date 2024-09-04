import GraphNode from "../../../model/GraphNode";
import { useEffect, useRef } from 'react'
import initGraphView from "./initGraphView";

export default function GraphView({
    subgraph,
    node
}:{
    subgraph:string,
    node:GraphNode
}) {
    let graphViewRef = useRef<HTMLDivElement>(null)

    useEffect(() => {
        if(graphViewRef.current) {
            initGraphView(graphViewRef.current, subgraph, node)
        }
    }, [ subgraph, node.getNodeId() ])

    return <div ref={graphViewRef} />
}




