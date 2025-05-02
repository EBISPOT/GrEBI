import { useState, useEffect } from "react"
import { getPaginated } from "../../app/api"
import { asArray, difference } from "../../app/util"
import GraphEdge from "../../model/GraphEdge"
import DatasourceSelector from "../DatasourceSelector"
import { DatasourceTags } from "../DatasourceTag"
import DataTable from "../datatable/DataTable"
import LoadingOverlay from "../LoadingOverlay"
import NodeRefLink from "../node_edge_list/NodeRefLink"
import { fchmod } from "fs"
import PropVal from "../../model/PropVal"
import PropVals from "../node_prop_table/PropVals"
import Refs from "../../model/Refs"


export interface ResultsState {
    total:number,
    results:any[],
    facetFieldToCounts:any
};

export default function ResultsTable({
    subgraph,
    queryid,
    extraSearchParams
}:{
    subgraph:string,
    queryid:string,
    extraSearchParams?: string[][]|undefined
}
) {


  let [resultsState, setResultsState] = useState<null|ResultsState>(null)

  let [dsEnabled,setDsEnabled] = useState<null|string[]>(null) 

  let [loading, setLoading] = useState(true)
  let [page, setPage] = useState(0)
  let [rowsPerPage, setRowsPerPage] = useState(10)
  let [filter, setFilter] = useState("")
  let [sortColumn, setSortColumn] = useState<undefined|string>(undefined)
  let [sortDir, setSortDir] = useState<'asc'|'desc'>("asc")

    useEffect(() => {
        async function getResults() {
            setLoading(true)
            let res = (await getPaginated<any>(`api/v1/subgraphs/${subgraph}/materialised_queries/${queryid}?${
                new URLSearchParams([
                    ['page', page],
                    ['size', rowsPerPage],
                    ...(sortColumn ? ['sortBy', sortColumn] : []),
                    ...(sortColumn ? ['sortDir', sortDir] : []),
                    ...(extraSearchParams||[]),
                    ...(filter ? [['q', filter]] : []),
                ] as any)
            }`))
            let newResultsState = {
                total: res.totalElements,
                results: res.elements,
                facetFieldToCounts:{}
            };
            setResultsState(newResultsState);
            setLoading(false)
        }
        getResults()

    }, [ subgraph, queryid, page, rowsPerPage, filter, sortColumn, sortDir ]);

    if(resultsState == null) {
        return <LoadingOverlay message="Loading results..." />
    }

    return <div>
        { loading && <LoadingOverlay message="Loading results..." /> }
        <DataTable
            addColumnsFromData={true}
            defaultSelector={DefaultSelector}
            data={resultsState.results}
            dataCount={resultsState.total}
            page={page}
            rowsPerPage={rowsPerPage}
            onRowsPerPageChange={setRowsPerPage}
            onPageChange={setPage}
            onFilter={setFilter}
            sortColumn={sortColumn}
            setSortColumn={setSortColumn}
            sortDir={sortDir}
            setSortDir={setSortDir}
            hideColumns={["_node_ids", "_refs", "id", "_version_"]}
        />
    </div>

}

function DefaultSelector(row:any, key:string) {
    let vals = asArray(row[key]).map(PropVal.from);

    console.dir(vals)

    if(!row['_refs']) {
        throw new Error("No refs in row")
    }

    return <PropVals 
     subgraph={row['subgraph']} 
    refs={new Refs(row['_refs'])}
    values={vals} />
}
