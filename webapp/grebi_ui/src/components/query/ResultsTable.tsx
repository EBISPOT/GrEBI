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
  graph: string;
  queryId: string;
  params: Record<string, any>|undefined;
  resultColumns: { column_id: string; column_type: string }[];
  // When true (a materialised template) show the free-text filter box; facets are
  // rendered whenever the backend returns them (materialised full templates only).
  materialised?: boolean;
}

export default function ResultsTable({ graph, queryId, params, resultColumns, materialised }: ResultsTableProps) {
  const [data, setData] = useState<any[]>([]);
  const [dataCount, setDataCount] = useState<number>(0);
  const [loading, setLoading] = useState(false);
  const [page, setPage] = useState(0);
  const [rowsPerPage, setRowsPerPage] = useState(10);
  const [sortColumn, setSortColumn] = useState<string>('');
  const [sortDir, setSortDir] = useState<'asc' | 'desc'>('asc');
  const [freeTextInput, setFreeTextInput] = useState<string>('');
  const [freeText, setFreeText] = useState<string>('');   // submitted value
  const [facets, setFacets] = useState<Record<string, Record<string, number>>>({});
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
      if (freeText) {
        reqParams.set('q', freeText);
      }
      reqParams.set('resolve', 'false');
      const response = await getPaginated<any>(
        `api/v1/graphs/${graph}/query/${queryId}`,
        reqParams
      );
      setData(response.elements);
      setDataCount(response.totalElements);
      setFacets((response.facetFieldsToCounts as any) || {});
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    if (params !== undefined) {
      fetchData();
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [params, page, rowsPerPage, sortColumn, sortDir, freeText]);

  function submitFilter(value: string) {
    setPage(0);
    setFreeText(value);
  }

  const columns: Column[] = resultColumns.map(col => ({
    id: col.column_id,
    name: col.column_type === 'EdgeId' ? '' : <OutputBadge>{col.column_id}</OutputBadge>,
    sortable: col.column_type !== 'EdgeId',
    selector: (row: any) => {
      const val = row[col.column_id];
      if (col.column_type === 'GraphNodeId') {
        if (!val) {
          return '-';
        }
        const node = new GraphNodeRef(val);
        return (
          <Link
            to={`/graphs/${graph}/nodes/${node.getEncodedNodeId()}`}
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
        if (val === undefined || val === null || val === '') {
          return '-';
        }
        return String(val);
      }
    }
  }));

  const facetEntries = Object.entries(facets || {})
    .filter(([, values]) => values && Object.keys(values).length > 0);

  const csvParams = new URLSearchParams(params as any);
  if (freeText) {
    csvParams.set('q', freeText);
  }

  if (loading) {
    return <LoadingOverlay message="Loading results..." />;
  }

  return (
<>
  <EdgeMetadataDialog
    open={edgeMetadata !== null}
    onClose={() => setEdgeMetadata(null)}
    graph={graph}
    edgeId={edgeMetadata?.edgeId || null}
  />

  {materialised &&
    <div className="mt-4 flex items-center gap-2">
      <input
        type="text"
        value={freeTextInput}
        onChange={(e) => setFreeTextInput(e.target.value)}
        onKeyDown={(e) => { if (e.key === 'Enter') submitFilter(freeTextInput); }}
        placeholder="Filter results…"
        className="border border-gray-300 rounded px-2 py-1 text-sm w-64"
      />
      <button
        className="px-3 py-1 border border-gray-300 text-sm font-medium rounded hover:bg-gray-50"
        onClick={() => submitFilter(freeTextInput)}
      >
        Filter
      </button>
      {freeText &&
        <button
          className="px-3 py-1 text-sm text-link-default hover:text-link-dark"
          onClick={() => { setFreeTextInput(''); submitFilter(''); }}
        >
          Clear
        </button>}
    </div>}

  {facetEntries.length > 0 &&
    <div className="mt-3 flex flex-wrap gap-3">
      {facetEntries.map(([col, values]) =>
        <div key={col} className="border border-gray-200 rounded p-2 text-sm min-w-[12rem]">
          <div className="mb-1"><OutputBadge>{col}</OutputBadge></div>
          <div className="max-h-40 overflow-y-auto pr-1">
            {Object.entries(values).map(([value, cnt]) =>
              <div key={value} className="flex justify-between gap-3">
                <span className="truncate" title={value}>{value}</span>
                <span className="text-neutral-500 tabular-nums">{cnt}</span>
              </div>)}
          </div>
        </div>)}
    </div>}

<div className="relative mt-4 w-full">

  <a href={process.env.REACT_APP_APIURL + `api/v1/graphs/${graph}/query/${queryId}.csv?` + csvParams.toString()}>
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
