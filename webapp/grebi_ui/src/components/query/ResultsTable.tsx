import { Box, Stack, Button } from "@mui/material";
import { useState, useEffect } from "react";
import { Link } from "react-router-dom";
import { getPaginated } from "../../app/api";
import GraphNodeRef from "../../model/GraphNodeRef";
import DataTable, { Column } from "../datatable/DataTable";
import LoadingOverlay from "../LoadingOverlay";
import { Download, Info } from "@mui/icons-material";
import OutputBadge from "../query/OutputBadge";
import EdgeMetadataDialog from "./EdgeMetadataDialog";

interface ResultsTableProps {
  subgraph: string;
  queryId: string;
  params: Record<string, any>|undefined;
  resultColumns: { column_id: string; column_type: string }[];
}

export default function ResultsTable({ subgraph, queryId, params, resultColumns }: ResultsTableProps) {
  const [data, setData] = useState<any[]>([]);
  const [dataCount, setDataCount] = useState<number>(0);
  const [loading, setLoading] = useState(false);
  const [page, setPage] = useState(0);
  const [rowsPerPage, setRowsPerPage] = useState(10);
  const [sortColumn, setSortColumn] = useState<string>('');
  const [sortDir, setSortDir] = useState<'asc' | 'desc'>('asc');
  const [filterKey, setFilterKey] = useState<string>('');
  const [edgeMetadata, setEdgeMetadata] = useState<{edgeId: string | null} | null>(null);

  async function fetchData() {
    setLoading(true);
    try {
      const reqParams = new URLSearchParams();
      Object.entries(params).forEach(([key, value]) => {
        if (value !== undefined && value !== null) {
          reqParams.set(key, value);
        }
      });
      reqParams.set('page', page.toString());
      reqParams.set('size', rowsPerPage.toString());
      if (sortColumn) {
        reqParams.set('sortBy', sortColumn);
        reqParams.set('sortDir', sortDir);
      }
      if (filterKey) {
        reqParams.set('filter', filterKey);
      }
      reqParams.set('resolve', 'false');
      const response = await getPaginated<any>(
        `api/v1/subgraphs/${subgraph}/query/${queryId}`,
        reqParams
      );
      setData(response.elements);
      setDataCount(response.totalElements);
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    if (params !== undefined) {
      fetchData();
    }
  }, [params, page, rowsPerPage, sortColumn, sortDir, filterKey]);

  const columns: Column[] = resultColumns.map(col => ({
    id: col.column_id,
    name: col.column_type === 'EdgeId' ? '' : <OutputBadge>{col.column_id}</OutputBadge>,
    sortable: col.column_type !== 'EdgeId',
    selector: (row: any) => {
      const val = row[col.column_id];
      if (col.column_type === 'GraphNodeId') {
        const node = new GraphNodeRef(val);
        return (
          <Link
            to={`/graphs/${subgraph}/nodes/${node.getEncodedNodeId()}`}
          >
            {node.getName()}
          </Link>
        );
      } else if (col.column_type === 'EdgeId') {
        return (
          <div className="flex justify-center">
            <button
              className="text-link-default hover:text-link-dark"
              title="View edge properties"
              onClick={(e) => {
                e.stopPropagation();
                setEdgeMetadata({edgeId: val || null});
              }}
            >
              <Info fontSize="medium" />
            </button>
          </div>
        );
      } else {
        return String(val);
      }
    }
  }));

  if(loading) {
    return <LoadingOverlay message="Loading results..." />;
  }

  return (
<>
  <EdgeMetadataDialog
    open={edgeMetadata !== null}
    onClose={() => setEdgeMetadata(null)}
    subgraph={subgraph}
    edgeId={edgeMetadata?.edgeId || null}
  />
<div className="relative mt-4 w-full">

  <a href={process.env.REACT_APP_APIURL + `api/v1/subgraphs/${subgraph}/query/${queryId}.csv?` + new URLSearchParams(params).toString()}>
  <button
    className="
      absolute top-2 right-4 z-10
      px-3 py-1
      border border-gray-300
      text-sm font-medium
      rounded
      hover:bg-gray-50
    "
  >
    <Download />
    &nbsp;
    All Results as CSV
  </button>
  </a>

  <DataTable
    columns={columns}
    defaultSelector={(row, key) => row[key]}
    data={data}
    dataCount={dataCount}
    placeholder={loading ? 'Loading...' : 'No results found'}
    page={page}
    rowsPerPage={rowsPerPage}
    onPageChange={setPage}
    onRowsPerPageChange={setRowsPerPage}
    sortColumn={sortColumn}
    setSortColumn={setSortColumn}
    sortDir={sortDir}
    setSortDir={setSortDir}
    addColumnsFromData={false}
  />
</div>
</>

  );
}
