
import React, { Fragment, useEffect, useState, useMemo } from "react";
import {useNavigate} from "react-router-dom";
import NodeRefLink from "./NodeRefLink";
import { getPaginated } from "../../app/api";
import { difference } from "../../app/util";
import PropVal from "../../model/PropVal";
import GraphEdge from "../../model/GraphEdge";
import GraphNode from "../../model/GraphNode";
import DatasourceSelector from "../DatasourceSelector";
import { DatasourceTags } from "../DatasourceTag";
import DataTable from "../datatable/DataTable";
import LoadingOverlay from "../LoadingOverlay";
import ErrorMessage from "../ErrorMessage";
import EdgeMetadataDialog from "../query/EdgeMetadataDialog";
import { Info } from "@mui/icons-material";

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
  let [error, setError] = useState<any>(null)
  let [openEdgeId, setOpenEdgeId] = useState<string|null>(null)
  let [page, setPage] = useState(0)
  let [rowsPerPage, setRowsPerPage] = useState(10)
  let [filter, setFilter] = useState("")
  let [sortColumn, setSortColumn] = useState("grebi:type")
  let [sortDir, setSortDir] = useState<'asc'|'desc'>("asc")

    const endpoint = direction === 'incoming' ? 'incoming_edges' : 'outgoing_edges'
    // the sort and narrowing of the list, shared by the page fetch and the CSV export
    const listParams = (): string[][] => [
        ['sortBy', sortColumn],
        ['sortDir', sortDir],
        ...(extraSearchParams||[]),
        ...(filter ? [['q', filter]] : []),
        ...(edgesState && dsEnabled!==null ?
                difference(edgesState.datasources, dsEnabled).map(ds => ['-grebi:datasources', ds]) : [])
    ]
    const csvHref = `${process.env.REACT_APP_APIURL}api/v1/graphs/${graph}/nodes/${node.getEncodedNodeId()}/${endpoint}.csv?${new URLSearchParams(listParams())}`

    useEffect(() => {
        async function getEdges() {
            setLoading(true)
            let res: any
            try {
                res = (await getPaginated<any>(`api/v1/graphs/${graph}/nodes/${node.getEncodedNodeId()}/${endpoint}?${
                new URLSearchParams([
                    ['page', page],
                    ['size', rowsPerPage],
                    ...listParams()
                ] as any)
            }`)).map(e => new GraphEdge(e))
            } catch (e) {
                setError(e)
                setLoading(false)
                return
            }
            setError(null)
            let facets = res.facetFieldsToCounts || {};
            let facetDatasources = Object.keys(facets['grebi:datasources'] || {});
            let newEdgesState = {
                total: res.totalElements,
                datasources: edgesState ? [...new Set([...edgesState.datasources, ...facetDatasources])] : facetDatasources,
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
        return error ? <ErrorMessage what="The edges" error={error} /> : <LoadingOverlay message="Loading edges..." />
    }

    return <div>
        <EdgeMetadataDialog open={openEdgeId !== null} onClose={() => setOpenEdgeId(null)} graph={graph} edgeId={openEdgeId} />
        { error && <ErrorMessage what="The edges" error={error} /> }
        <div className="pb-5 flex items-center justify-between gap-4">
        <DatasourceSelector datasources={edgesState.datasources} dsEnabled={dsEnabled!==null?dsEnabled:edgesState.datasources} setDsEnabled={setDsEnabled} />
        {edgesState.total > 0 && (
          <a className="link-default text-sm whitespace-nowrap" href={csvHref} title="Every edge of this list, as a CSV file">
            Download as CSV
          </a>
        )}
        </div>
        { loading && <LoadingOverlay message="Loading edges..." /> }
        <DataTable columns={[
                {
                    id: 'grebi:edgeId',
                    name: '',
                    selector: (row:GraphEdge) => {
                        return <button
                            className="text-link-default hover:text-link-dark"
                            title="View edge properties"
                            aria-label={`View edge ${row.getEdgeId()}`}
                            onClick={(e) => { e.stopPropagation(); setOpenEdgeId(row.getEdgeId()) }}
                        >
                            <Info fontSize="small" />
                        </button>
                    },
                    sortable: false,
                },
                {
                    id: 'grebi:datasources',
                    name: 'Datasources',
                    selector: (row:GraphEdge) => {
                        return <DatasourceTags dss={row.getDatasources()} linked />
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
                        selector: (row: GraphEdge) => {
                            // edge properties live on the GraphEdge's props
                            return <div>{PropVal.arrFrom(row.props?.[prop]).map(v => String(v.value)).join(', ')}</div>
                        },
                    }
                }) as any
            ]}
            defaultSelector={(row:any,key:string)=>row.props?.[key]}
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
