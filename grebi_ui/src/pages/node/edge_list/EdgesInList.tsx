
import { Grid } from "@mui/material";
import GraphNode from "../../../model/GraphNode";
import React, { Fragment, useEffect, useState, useMemo } from "react";
import { get } from "../../../app/api";
import {MaterialReactTable, MRT_ColumnDef, useMaterialReactTable} from "material-react-table";
import DatasourceSelector from "../../../components/DatasourceSelector";
import LoadingOverlay from "../../../components/LoadingOverlay";
import {useNavigate} from "react-router-dom";
import NodeRefLink from "./NodeRefLink";
import GraphNodeRef from "../../../model/GraphNodeRef";
import { DatasourceTags } from "../../../components/DatasourceTag";
import DataTable from "../../../components/datatable/DataTable";
import { Record } from "immutable";
import { dir } from "console";

function difference(a:any[], b:any[]) {
    return a.filter(x => b.indexOf(x) === -1)
}

export default function EdgesInList(params:{
    subgraph:string,
    node:GraphNode
}) {
    let { subgraph, node } = params

    const navigate = useNavigate();

  let [edgesState, setEdgesState] = useState<null|{
    total:number,
    datasources:string[],
    edges:any[],
    facetFieldToCounts:any,
    propertyColumns:string[]
  }>(null)

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
            let res = await get<any>(`api/v1/subgraphs/${subgraph}/nodes/${node.getNodeId()}/incoming_edges?${
                new URLSearchParams([
                    ['page', page],
                    ['size', rowsPerPage],
                    ['sortBy', sortColumn],
                    ['sortDir', sortDir],
                    ...(filter ? [['q', filter]] : []),
                    ...(edgesState && dsEnabled!==null ? 
                            difference(edgesState.datasources, dsEnabled).map(ds => ['-grebi:datasources', ds]) : [])
                ])
            }`)
            setEdgesState({
                total: res['total'],
                datasources: Object.keys(res['facetFieldToCounts']['grebi:datasources']),
                edges: res['content'],
                facetFieldToCounts: res['facetFieldToCounts'],
                propertyColumns:
                    Object.keys(res['facetFieldToCounts'])
                        .filter(k => k !== 'grebi:datasources')
                        .filter(k => Object.entries(res['facetFieldToCounts'][k]).length > 0)
            })
            setLoading(false)
        }
        getEdges()

    }, [ node.getNodeId(), JSON.stringify(dsEnabled), page, rowsPerPage, filter, sortColumn, sortDir ]);

    if(edgesState == null) {
        return <LoadingOverlay message="Loading edges..." />
    }


    return <div>
        <DatasourceSelector datasources={edgesState.datasources} dsEnabled={dsEnabled!==null?dsEnabled:edgesState.datasources} setDsEnabled={setDsEnabled} />
        { loading && <LoadingOverlay message="Loading edges..." /> }
        <DataTable columns={[
                {
                    id: 'grebi:datasources',
                    name: 'Datasources',
                    selector: (row:any) => {
                        return <DatasourceTags dss={row['grebi:datasources']} />
                    },
                    sortable: true,
                },
                {
                    id: 'grebi:from',
                    name: 'Source',
                    selector: (row:any) => {
                        return  <NodeRefLink subgraph={subgraph} nodeRef={new GraphNodeRef(row['grebi:from'])} />
                    },
                    sortable: true,
                },
                {
                    id: 'grebi:type',
                    name: 'Type',
                    selector: (row:any) => {
                        return row['grebi:type']
                    },
                    sortable: true,
                },
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
