
import React, { Fragment, useEffect, useState, useMemo } from "react";
import {useNavigate} from "react-router-dom";
import NodeRefLink from "./NodeRefLink";
import { getPaginated } from "../../app/api";
import { difference } from "../../app/util";
import GraphEdge from "../../model/GraphEdge";
import GraphNode from "../../model/GraphNode";
import DatasourceSelector from "../DatasourceSelector";
import { DatasourceTags } from "../DatasourceTag";
import DataTable from "../datatable/DataTable";
import LoadingOverlay from "../LoadingOverlay";
import { dir } from "console";

export interface EdgesState {
    total:number,
    datasources:string[],
    edges:any[],
    facetFieldToCounts:any,
    propertyColumns:string[]
};

export default function EdgesList(params:{
    graph:string,
    node:GraphNode,
    direction:'incoming'|'outgoing',
    onEdgesLoaded?:((edges:EdgesState) => void)|undefined,
    extraSearchParams?: string[][]|undefined
}) {
    let { direction, graph, node, onEdgesLoaded, extraSearchParams } = params

  let [edgesState, setEdgesState] = useState<null|EdgesState>(null)

  let [dsEnabled,setDsEnabled] = useState<null|string[]>(null) 

  let [loading, setLoading] = useState(true)
  let [page, setPage] = useState(0)
  let [rowsPerPage, setRowsPerPage] = useState(10)
  let [filter, setFilter] = useState("")
  let [sortColumn, setSortColumn] = useState("grebi:type")
  let [sortDir, setSortDir] = useState<'asc'|'desc'>("asc")

    useEffect(() => {
        async function getEdges() {
            console.log('refreshing ', node.getNodeId(), JSON.stringify(dsEnabled), JSON.stringify(edgesState?.datasources))
            setLoading(true)
            let endpoint = direction === 'incoming' ? 'incoming_edges' : 'outgoing_edges'
            let res = (await getPaginated<any>(`api/v1/graphs/${graph}/nodes/${node.getEncodedNodeId()}/${endpoint}?${
                new URLSearchParams([
                    ['page', page],
                    ['size', rowsPerPage],
                    ['sortBy', sortColumn],
                    ['sortDir', sortDir],
                    ...(extraSearchParams||[]),
                    ...(filter ? [['q', filter]] : []),
                    ...(edgesState && dsEnabled!==null ? 
                            difference(edgesState.datasources, dsEnabled).map(ds => ['-grebi:datasources', ds]) : [])
                ])
            }`)).map(e => new GraphEdge(e))
            let facets = res.facetFieldsToCounts || {};
            let newEdgesState = {
                total: res.totalElements,
                datasources: Object.keys(facets['grebi:datasources'] || {}),
                edges: res.elements,
                facetFieldToCounts: facets,
                propertyColumns:
                    Object.keys(facets)
                        .filter(k => k !== 'grebi:datasources')
                        .filter(k => Object.entries(facets[k] || {}).length > 0)
            };
            if(onEdgesLoaded)
                onEdgesLoaded(newEdgesState);
            setEdgesState(newEdgesState);
            setLoading(false)
        }
        getEdges()

    }, [ direction, node.getNodeId(), JSON.stringify(dsEnabled), page, rowsPerPage, filter, sortColumn, sortDir ]);

    if(edgesState == null) {
        return <LoadingOverlay message="Loading edges..." />
    }

    return <div>
        <div className="pb-5">
        <DatasourceSelector datasources={edgesState.datasources} dsEnabled={dsEnabled!==null?dsEnabled:edgesState.datasources} setDsEnabled={setDsEnabled} />
        </div>
        { loading && <LoadingOverlay message="Loading edges..." /> }
        <DataTable columns={[
                {
                    id: 'grebi:datasources',
                    name: 'Datasources',
                    selector: (row:GraphEdge) => {
                        return <DatasourceTags dss={row.getDatasources()} />
                    },
                    sortable: true,
                },
                ...
                (direction === 'incoming' ? [
                    {
                    id: 'grebi:from',
                    name: 'From Node',
                    selector: (row:GraphEdge) => {
                        return  <NodeRefLink graph={graph} nodeRef={row.getFrom()} />
                    },
                    sortable: true,
                } ,
                {
                    id: 'grebi:type',
                    name: 'Edge Type',
                    selector: (row:GraphEdge) => {
                        return <code>{row.getType()}</code>
                    },
                    sortable: true,
                }
            ] : [
                {
                    id: 'grebi:type',
                    name: 'Edge Type',
                    selector: (row:GraphEdge) => {
                        return <code>{row.getType()}</code>
                    },
                    sortable: true,
                },
                 {
                    id: 'grebi:to',
                    name: 'To Node',
                    selector: (row:GraphEdge) => {
                        return  <NodeRefLink graph={graph} nodeRef={row.getTo()} />
                    },
                    sortable: true,
                }
            ]),
                ...(edgesState?.propertyColumns || []).map((prop:string) => {
                    return {
                        name: prop,
                        // filterFn: 'includesString',
                        // filterVariant: 'multi-select',
                        // filterSelectOptions: edgesState?.facetFieldToCounts[prop] || [],
                        selector: (row) => {
                            return <div>{row[prop]}</div>
                        },
                    }
                }) as any
            ]}
            defaultSelector={(row:any,key:string)=>row[key]}
            data={edgesState.edges}
            dataCount={edgesState.total}
            page={page}
            rowsPerPage={rowsPerPage}
            onRowsPerPageChange={setRowsPerPage}
            onPageChange={setPage}
            onFilter={setFilter}
            sortColumn={sortColumn}
            setSortColumn={setSortColumn}
            sortDir={sortDir}
            setSortDir={setSortDir}
        />
    </div>


}
