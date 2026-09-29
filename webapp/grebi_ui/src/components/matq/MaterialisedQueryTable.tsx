import { useState, useEffect, Fragment } from "react";
import MaterialisedTable from "../../model/MaterialisedTable";
import LocalDataTable from "../datatable/LocalDataTable"
import { get } from "../../app/api";
import { CircularProgress } from "@mui/material";
import { Download } from "@mui/icons-material";
import { Link } from "react-router-dom";
import ErrorMessage from "../ErrorMessage";
import { tableFileUrl, TableFormat } from "../../app/ftp";

/** Where a table is looked at: the query it is the results of, or its own page. */
function pageOf(table:MaterialisedTable):string {
    return table.kind === "standalone"
        ? `/graphs/${table.graph}/tables/${table.id}`
        : `/graphs/${table.graph}/queries/${table.id}`
}

function DownloadLink({ table, format, label }:{ table:MaterialisedTable, format:TableFormat, label:string }) {
    return <a className="link-default inline-flex items-center gap-1 mr-4 whitespace-nowrap"
            href={tableFileUrl(table.graph, table.id, format)}
            target="_blank" rel="noopener noreferrer">
        <Download fontSize="small" /> {label}
    </a>
}

// The rows come by table id from the API. None of the columns is sortable:
// LocalDataTable keeps a sort state but does not sort its rows.
const cols= [
    {
        id:"id",
        name:"Table",
        selector:(row:MaterialisedTable)=> {
            return <Fragment>
                <Link className="link-default" to={pageOf(row)}><code>{row.id}</code></Link>
                {row.title && <div className="text-sm text-gray-700">{row.title}</div>}
                </Fragment>
        },
        sortable:false
    },
    {
        id:"num_rows",
        name:"Rows",
        selector:(row:MaterialisedTable)=> row.num_rows === undefined ? "" : row.num_rows.toLocaleString("en-GB"),
        sortable:false
    },
    {
        id:"columns",
        name:"Columns",
        selector:(row:MaterialisedTable)=> <span className="text-sm text-gray-700">{row.columns.join(", ")}</span>,
        sortable:false
    },
    {
        id:"download",
        name:"Download",
        selector:(row:MaterialisedTable)=> <Fragment>
                <DownloadLink table={row} format="csv" label="CSV" />
                <DownloadLink table={row} format="parquet" label="Parquet" />
            </Fragment>,
        sortable:false
    }
];


/** The materialised tables of a graph, or of every graph, with their files in the latest release. */
export default function MaterialisedQueryTable({
    graph
}:{
    graph?:string|undefined
}) {

  let [tables, setTables] = useState<MaterialisedTable[]|null>(null);
  let [error, setError] = useState<any>(null);

    useEffect(() => {
        setError(null);
        get<MaterialisedTable[]>(graph ? `api/v1/graphs/${graph}/tables` : `api/v1/tables`).then(r => setTables(r)).catch(setError);
    }, [graph]);

    if(error) {
        return <ErrorMessage what="The tables" error={error} />
    }

    if(!tables) {
        return <CircularProgress />
    }

    return <LocalDataTable
                    data={tables}
                    addColumnsFromData={false}
                    defaultSelector={(row,key)=>row[key]}
                    columns={cols}
                    />

}
