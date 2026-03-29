import { useEffect, useState } from "react"
import { Link } from "react-router-dom"
import GraphNode from "../model/GraphNode"
import { getPaginated, Page, get } from "../app/api"
import NodeRefLink from "./node_edge_list/NodeRefLink"
import GraphNodeRef from "../model/GraphNodeRef"
import encodeNodeId from "../encodeNodeId"

type SimilarResult = {
    node: GraphNodeRef,
    score: number
}

export default function NodeSimilarList(params:{
    graph:string,
    node:GraphNodeRef,
    model?:string
}) {
    let { graph, node, model } = params
    
  let [loading, setLoading] = useState(true)
  let [results, setResults] = useState<SimilarResult[]|null>(null)

    useEffect(() => {
        async function getSimilar() {
            setLoading(true)
            const params: string[][] = [['n', '20']];
            if (model) params.push(['model', model]);
            let res = await get(`api/v1/graphs/${graph}/nodes/${node.getEncodedNodeId()}/similar?${
                new URLSearchParams(params)
            }`)
            setLoading(false)
            return (res as any[]).map(r => {
                // Handle both old format ({node, score}) and new postgres format ({nodeId, name, distance, ...})
                if (r.node) {
                    return { node: new GraphNodeRef(r.node), score: r.score };
                }
                return {
                    node: new GraphNodeRef({
                        'grebi:nodeId': r.nodeId,
                        'grebi:name': r.name,
                        'grebi:datasources': r.datasources,
                        'grebi:type': r.type,
                        'grebi:sourceIds': r.sourceIds,
                    }),
                    score: r.distance != null ? (1 - r.distance) : 0
                };
            })
        }

        getSimilar().then(r => {
            setResults(r)
        })

    }, [ graph, node, model ])

    if(loading || !results) {
        return <div className="spinner-default w-7 h-7" />
    }

    if(!results || results!.length === 0) {
        return <div>No similar nodes found; likely this node is not in the embedding database</div>
    }

    return <table className="w-full text-sm border border-gray-200 rounded-lg overflow-hidden">
      <thead>
        <tr className="bg-gray-100 text-left text-gray-600 border-b border-gray-200">
          <th className="py-2 px-3 font-medium">Node</th>
          <th className="py-2 px-3 font-medium text-right">Similarity Score</th>
        </tr>
      </thead>
      <tbody>
        {results.map((row, i) => (
          <tr key={row.node.getNodeId()} className={`hover:bg-blue-50 cursor-pointer transition-colors ${i % 2 === 1 ? 'bg-gray-50' : ''}`}>
            <td className="py-2 px-3" colSpan={2}>
              <Link to={`/graphs/${graph}/nodes/${encodeNodeId(row.node.getNodeId())}`} className="flex justify-between items-center no-underline text-inherit">
                <span className="text-blue-600">{row.node.getName()}</span>
                <span className="text-gray-500 tabular-nums">{row.score.toFixed(4)}</span>
              </Link>
            </td>
          </tr>
        ))}
      </tbody>
    </table>
}

