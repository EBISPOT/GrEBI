import { useEffect, useState } from "react";
import DataTable, { Column } from "./DataTable";

export default function LocalDataTable({
    data,
    columns,
    hideColumns,
    addColumnsFromData,
    defaultSelector,
    onSelectRow,
    maxRowHeight
}:{
    data: any[],
    columns?: readonly Column[]|undefined,
    hideColumns?: string[]|undefined,
    addColumnsFromData?:boolean,
    defaultSelector:undefined|((row:any, key:string)=>any);
    onSelectRow?: (row: any) => void,
    maxRowHeight?:string|undefined
}) {

    let [page, setPage] = useState(0)
    let [rowsPerPage, setRowsPerPage] = useState(10)
    let [sortColumn, setSortColumn] = useState<string|undefined>("")
    let [sortDir, setSortDir] = useState<'asc'|'desc'>("asc")
    let [filter, setFilter] = useState<string>("")

    const filtered = data.filter(row => {
        return Object.values(row).some(v => {
            return (v+'').toLowerCase().includes(filter.toLowerCase())
        })
    });

    // Pagination is 0-based; the count must be of the filtered rows, not the page
    const pageRows = filtered.slice(page*rowsPerPage, (page+1)*rowsPerPage)

    return <DataTable
        data={pageRows}
        columns={columns}
        addColumnsFromData={addColumnsFromData}
        hideColumns={hideColumns}
        dataCount={filtered.length}
        defaultSelector={defaultSelector}
        onFilter={(f) => { setFilter(f); setPage(0) }}
        rowsPerPage={rowsPerPage}
        onRowsPerPageChange={setRowsPerPage}
        page={page}
        onPageChange={setPage}
        sortColumn={sortColumn}
        setSortColumn={setSortColumn}
        sortDir={sortDir}
        setSortDir={setSortDir}
        onSelectRow={onSelectRow}
        maxRowHeight={maxRowHeight}
    />
}
