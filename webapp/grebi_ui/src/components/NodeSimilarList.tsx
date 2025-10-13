import { useEffect, useState } from "react"
import GraphNode from "../model/GraphNode"
import { getPaginated, Page, get } from "../app/api"
import DataTable from "./datatable/DataTable"
import NodeRefLink from "./node_edge_list/NodeRefLink"
import GraphNodeRef from "../model/GraphNodeRef"

type SimilarResult = {
    node: GraphNodeRef,
    score: number
}

export default function NodeSimilarList(params:{
    subgraph:string,
    node:GraphNodeRef
}) {
    let { subgraph, node } = params
    
  let [loading, setLoading] = useState(true)
  let [results, setResults] = useState<SimilarResult[]|null>(null)

    useEffect(() => {
        async function getSimilar() {
            setLoading(true)
            let res = await get(`api/v1/subgraphs/${subgraph}/nodes/${node.getEncodedNodeId()}/similar?${
                new URLSearchParams([
                    ['n', '20']
                ])
            }`)
            setLoading(false)
            return (res as any[]).map(r => ({
                node: new GraphNodeRef(r.node),
                score: r.score
            }))
        }

        getSimilar().then(r => {
            setResults(r)
        })

    }, [ subgraph, node ])

    console.dir(results)

    if(loading || !results) {
        return <div className="spinner-default w-7 h-7" />
    }

    if(!results || results!.length === 0) {
        return <div>No similar nodes found; likely this node is not in the embedding database</div>
    }

    return <DataTable
            defaultSelector={(row:any,key:string)=>row[key]}
            data={results}
    columns={[
                 {
                    id: 'node',
                    name: 'Node',
                    selector: (row:SimilarResult) => {
                        return  <NodeRefLink subgraph={subgraph} nodeRef={row.node} />
                    },
                    sortable: true,
                },
                {
                    id: 'score',
                    name: 'Similarity Score',
                    selector: (row:SimilarResult) => row.score.toFixed(4),
                    sortable: true,
                }
    ]} />
}

