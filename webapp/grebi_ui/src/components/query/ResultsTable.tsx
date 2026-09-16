import { useState, useEffect, useRef } from "react";
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

// Facet selections: result column -> the values ticked. Values ticked in one
// column are alternatives; columns combine. Sent as repeated `<column>=<value>`
// query params, which the API honours on the materialised path.
type Selections = Record<string, string[]>;

const FILTER_DEBOUNCE_MS = 350;

export default function ResultsTable({ graph, queryId, params, resultColumns, materialised }: ResultsTableProps) {
  const [data, setData] = useState<any[]>([]);
  const [dataCount, setDataCount] = useState<number>(0);
  const [loading, setLoading] = useState(false);
  const [page, setPage] = useState(0);
  const [rowsPerPage, setRowsPerPage] = useState(10);
  const [sortColumn, setSortColumn] = useState<string>('');
  const [sortDir, setSortDir] = useState<'asc' | 'desc'>('asc');
  const [freeTextInput, setFreeTextInput] = useState<string>('');
  const [freeText, setFreeText] = useState<string>('');   // applied value
  const [selections, setSelections] = useState<Selections>({});
  const [facets, setFacets] = useState<Record<string, Record<string, number>>>({});
  const [edgeMetadata, setEdgeMetadata] = useState<{edgeId: string | null} | null>(null);
  // Serial of the latest request, so a slow earlier response cannot overwrite
  // a newer one (facets can be ticked faster than a large closure answers).
  const requestSeq = useRef(0);

  // The template parameters plus the narrowing the user has applied; shared by
  // the page fetch and the CSV export so the file matches the table.
  function narrowedParams(): URLSearchParams {
    const p = new URLSearchParams();
    Object.entries(params || {}).forEach(([key, value]) => {
      if (value !== undefined && value !== null) {
        p.set(key, value);
      }
    });
    if (freeText) {
      p.set('q', freeText);
    }
    Object.entries(selections).forEach(([col, values]) => {
      values.forEach(v => p.append(col, v));
    });
    return p;
  }

  async function fetchData() {
    const seq = ++requestSeq.current;
    setLoading(true);
    try {
      const reqParams = narrowedParams();
      reqParams.set('page', page.toString());
      reqParams.set('size', rowsPerPage.toString());
      if (sortColumn) {
        reqParams.set('sortBy', sortColumn);
        reqParams.set('sortDir', sortDir);
      }
      reqParams.set('resolve', 'false');
      const response = await getPaginated<any>(
        `api/v1/graphs/${graph}/query/${queryId}`,
        reqParams
      );
      if (seq !== requestSeq.current) {
        return;
      }
      setData(response.elements);
      setDataCount(response.totalElements);
      setFacets((response.facetFieldsToCounts as any) || {});
    } finally {
      if (seq === requestSeq.current) {
        setLoading(false);
      }
    }
  }

  useEffect(() => {
    if (params !== undefined) {
      fetchData();
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [params, page, rowsPerPage, sortColumn, sortDir, freeText, selections]);

  // A new query (or new inputs) starts from an unnarrowed table.
  useEffect(() => {
    setSelections(prev => Object.keys(prev).length > 0 ? {} : prev);
  }, [params, queryId]);

  // Filter as you type, after a short pause; Enter applies straight away.
  useEffect(() => {
    const t = setTimeout(() => applyFreeText(freeTextInput), FILTER_DEBOUNCE_MS);
    return () => clearTimeout(t);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [freeTextInput]);

  function applyFreeText(value: string) {
    const v = value.trim();
    if (v === freeText) {
      return;
    }
    setPage(0);
    setFreeText(v);
  }

  function toggleSelection(col: string, value: string) {
    setSelections(prev => {
      const current = prev[col] || [];
      const next = current.includes(value) ? current.filter(v => v !== value) : [...current, value];
      const out = { ...prev };
      if (next.length > 0) {
        out[col] = next;
      } else {
        delete out[col];
      }
      return out;
    });
    setPage(0);
  }

  function clearNarrowing() {
    setFreeTextInput('');
    setFreeText('');
    setSelections({});
    setPage(0);
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
      } else if (col.column_type === 'PubmedId') {
        // a bare PubMed id (the templates strip the pubmed: prefix)
        if (val === undefined || val === null || val === '') {
          return '-';
        }
        const pmid = String(val).replace(/^pubmed:/i, '');
        return (
          <a
            className="link-default"
            href={`https://pubmed.ncbi.nlm.nih.gov/${pmid}/`}
            target="_blank"
            rel="noopener noreferrer"
            onClick={(e) => e.stopPropagation()}
          >
            {pmid}
          </a>
        );
      } else {
        if (val === undefined || val === null || val === '') {
          return '-';
        }
        return String(val);
      }
    }
  }));

  // Facet columns in result-column order: those with a breakdown, plus any
  // whose ticked values must stay visible even when the breakdown is empty.
  const facetColumns = Array.from(new Set([
    ...resultColumns.map(c => c.column_id),
    ...Object.keys(facets || {}),
    ...Object.keys(selections)
  ])).filter(col =>
    (facets[col] && Object.keys(facets[col]).length > 0) || (selections[col] || []).length > 0
  );

  // A column's values with counts; ticked values outside the top-N breakdown
  // are listed without a count so they can be unticked.
  function facetValues(col: string): [string, number | undefined][] {
    const counts = facets[col] || {};
    const rows: [string, number | undefined][] = Object.entries(counts);
    for (const v of selections[col] || []) {
      if (!(v in counts)) {
        rows.push([v, undefined]);
      }
    }
    return rows;
  }

  const hasNarrowing = !!freeText || Object.keys(selections).length > 0;
  const showSidebar = !!materialised || facetColumns.length > 0;

  return (
<>
  <EdgeMetadataDialog
    open={edgeMetadata !== null}
    onClose={() => setEdgeMetadata(null)}
    graph={graph}
    edgeId={edgeMetadata?.edgeId || null}
  />

  <div className="mt-4 flex flex-col lg:flex-row gap-6 items-start">

    {showSidebar &&
      <aside className="w-full lg:w-72 shrink-0 border border-gray-200 rounded p-3 text-sm">
        <div className="flex items-center justify-between mb-2">
          <span className="font-bold">Filter results</span>
          {hasNarrowing &&
            <button
              className="text-xs text-link-default hover:text-link-dark"
              onClick={clearNarrowing}
            >
              Clear all
            </button>}
        </div>
        {materialised &&
          <input
            type="text"
            value={freeTextInput}
            onChange={(e) => setFreeTextInput(e.target.value)}
            onKeyDown={(e) => { if (e.key === 'Enter') applyFreeText(freeTextInput); }}
            placeholder="Filter results…"
            className="border border-gray-300 rounded px-2 py-1 text-sm w-full mb-3"
          />}
        {facetColumns.map(col =>
          <div key={col} className="mb-3">
            <div className="mb-1"><OutputBadge size="xs">{col}</OutputBadge></div>
            <div className="max-h-56 overflow-y-auto pr-1">
              {facetValues(col).map(([value, cnt]) => {
                const ticked = (selections[col] || []).includes(value);
                return (
                  <label
                    key={value}
                    className="flex items-center gap-2 py-0.5 rounded cursor-pointer hover:bg-neutral-50"
                    title={value}
                  >
                    <input
                      type="checkbox"
                      className="shrink-0"
                      checked={ticked}
                      onChange={() => toggleSelection(col, value)}
                    />
                    <span className={`truncate flex-1 ${ticked ? 'font-bold' : ''}`}>{value}</span>
                    {cnt !== undefined &&
                      <span className="text-neutral-500 tabular-nums">{cnt}</span>}
                  </label>
                );
              })}
            </div>
          </div>)}
      </aside>}

    <div className="relative flex-1 min-w-0 w-full min-h-[10rem]">

      {loading && <LoadingOverlay scoped message="Loading results..." />}

      <a href={process.env.REACT_APP_APIURL + `api/v1/graphs/${graph}/query/${queryId}.csv?` + narrowedParams().toString()}>
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

      {!loading && dataCount === 0 && (
        <div className="px-4 py-2 text-sm text-neutral-default">No results found</div>
      )}
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
  </div>
</>
  );
}
