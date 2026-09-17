import React, { useCallback, useEffect, useState, useRef } from "react";
import { useParams, useSearchParams } from "react-router-dom";
import { get, getPaginated } from "../../../app/api";
import EbiBreadcrumbsBar from "../EbiBreadcrumbsBar";
import GraphEdge from "../../../model/GraphEdge";
import GraphNodeRef from "../../../model/GraphNodeRef";
import NodeRefLink from "../../../components/node_edge_list/NodeRefLink";
import NodeSelectorBox from "../../../components/NodeSelectorBox";
import { DatasourceTags } from "../../../components/DatasourceTag";
import LoadingOverlay from "../../../components/LoadingOverlay";
import ErrorMessage from "../../../components/ErrorMessage";
import EdgeMetadataDialog from "../../../components/query/EdgeMetadataDialog";
import encodeNodeId from "../../../encodeNodeId";
import { Pagination } from "@mui/material";
import { Close, KeyboardArrowDown, Info, ArrowUpward, ArrowDownward } from "@mui/icons-material";

function hasFacetData(f: any): boolean {
  return Object.keys(f || {}).some(k => Object.keys(f[k] || {}).length > 0);
}

const TYPE = "grebi:type";
const DATASOURCES = "grebi:datasources";
const FROM = "grebi:fromNodeId";
const TO = "grebi:toNodeId";

/**
 * Edges of a graph, narrowed by any number of types and datasources (each a
 * list of alternatives), by their end nodes, and sorted by type; every filter
 * lives in the URL.
 */
export default function EbiEdgeSearchPage() {
  let params = useParams();
  const graph: string = params.graph as string;

  const [searchParams, setSearchParams] = useSearchParams();

  let [loading, setLoading] = useState(true);
  let [error, setError] = useState<any>(null);
  let [openEdgeId, setOpenEdgeId] = useState<string | null>(null);
  let [edges, setEdges] = useState<GraphEdge[]>([]);
  let [totalResults, setTotalResults] = useState(0);
  let [page, setPage] = useState(0);
  let [rowsPerPage] = useState(20);
  let [facets, setFacets] = useState<any>({});

  // Filters and sort, all read from the URL
  const typeFilters = searchParams.getAll(TYPE);
  const dsFilters = searchParams.getAll(DATASOURCES);
  const fromNodeId = searchParams.get(FROM);
  const toNodeId = searchParams.get(TO);
  const sortDir = searchParams.get("sortDir") === "desc" ? "desc" : "asc";
  const filterKey = [typeFilters.join("\u0001"), dsFilters.join("\u0001"), fromNodeId, toNodeId, sortDir].join("|");

  // Facet state
  const typeFacets: Record<string, number> = facets?.[TYPE] || {};
  const dsFacets: Record<string, number> = facets?.[DATASOURCES] || {};
  const [hideFilters, setHideFilters] = useState(false);

  const hasFilters = typeFilters.length > 0 || dsFilters.length > 0 || !!fromNodeId || !!toNodeId;

  // The end nodes named in the URL, resolved so their names can be shown
  const [endNodes, setEndNodes] = useState<Record<string, GraphNodeRef>>({});
  useEffect(() => {
    for (const nodeId of [fromNodeId, toNodeId]) {
      if (!nodeId || endNodes[nodeId]) continue;
      get<any>(`api/v1/graphs/${graph}/nodes/${encodeNodeId(nodeId)}`)
        .then((props) => setEndNodes((prev) => ({ ...prev, [nodeId]: new GraphNodeRef(props) })))
        .catch(() => setEndNodes((prev) => ({ ...prev, [nodeId]: new GraphNodeRef({ "grebi:nodeId": nodeId }) })));
    }
  }, [graph, fromNodeId, toNodeId]);

  // Fetch stats for sidebar facets (used when API skips expensive facet computation)
  const [statsFacets, setStatsFacets] = useState<any>({});
  // read by the edge fetch without being one of its triggers, so the arrival
  // of the stats does not re-run the search
  const statsFacetsRef = useRef<any>({});
  useEffect(() => {
    get<any>(`api/v1/graphs/${graph}/stats`).then((stats) => {
      const f: any = {};
      if (stats.edge_counts_by_type) f[TYPE] = stats.edge_counts_by_type;
      if (stats.edge_counts_by_datasource) f[DATASOURCES] = stats.edge_counts_by_datasource;
      statsFacetsRef.current = f;
      setStatsFacets(f);
    }).catch(() => {});
  }, [graph]);
  // stats arriving after a search that had no facets of its own fill them in
  useEffect(() => {
    setFacets((prev: any) => hasFacetData(prev) ? prev : statsFacets);
  }, [statsFacets]);

  useEffect(() => {
    setPage(0);
  }, [filterKey]);

  useEffect(() => {
    async function fetchEdges() {
      setLoading(true);
      let params: string[][] = [
        ["page", String(page)],
        ["size", String(rowsPerPage)],
        ["sortBy", TYPE],
        ["sortDir", sortDir],
      ];
      for (const t of typeFilters) params.push([TYPE, t]);
      for (const ds of dsFilters) params.push([DATASOURCES, ds]);
      if (fromNodeId) params.push([FROM, fromNodeId]);
      if (toNodeId) params.push([TO, toNodeId]);

      let res;
      try {
        res = await getPaginated<any>(
          `api/v1/graphs/${graph}/edges?${new URLSearchParams(params)}`
        );
      } catch (e) {
        setError(e);
        setEdges([]);
        setTotalResults(0);
        setLoading(false);
        return;
      }
      setError(null);
      setEdges(res.elements.map((e: any) => new GraphEdge(e)));
      setTotalResults(res.totalElements);
      const apiFacets = res.facetFieldsToCounts || {};
      // Use API facets if returned, otherwise fall back to stats
      setFacets(hasFacetData(apiFacets) ? apiFacets : statsFacetsRef.current);
      setLoading(false);
    }
    fetchEdges();
  }, [graph, filterKey, page, rowsPerPage]);

  const updateParams = useCallback(
    (change: (next: URLSearchParams) => void) => {
      const next = new URLSearchParams(searchParams);
      change(next);
      setSearchParams(next);
    },
    [searchParams, setSearchParams]
  );

  /** Adds the value to a list filter, or removes it when it is already there. */
  const toggleValue = useCallback(
    (key: string, value: string) => updateParams((next) => {
      const values = next.getAll(key);
      next.delete(key);
      for (const v of values.includes(value) ? values.filter((v) => v !== value) : [...values, value]) {
        next.append(key, v);
      }
    }),
    [updateParams]
  );

  const setSingle = useCallback(
    (key: string, value: string | null) => updateParams((next) => {
      if (value) {
        next.set(key, value);
      } else {
        next.delete(key);
      }
    }),
    [updateParams]
  );

  const totalPages = Math.ceil(totalResults / rowsPerPage);

  const breadcrumbs = [
    { url: `/graphs`, label: "Graphs" },
    { url: `/graphs/${graph}/edges`, label: "Edges" },
  ];

  const chip = (key: string, label: string, value: string, text: string, colour: string) => (
    <span key={`${key}:${value}`} className={`inline-flex items-center gap-1 px-3 py-1 ${colour} rounded-full text-sm`}>
      {label}: {text}
      <button
        onClick={() => (key === FROM || key === TO ? setSingle(key, null) : toggleValue(key, value))}
        aria-label={`Remove ${label} ${text}`}
        className="hover:opacity-70"
      >
        <Close fontSize="small" />
      </button>
    </span>
  );

  const endNodeName = (nodeId: string) => endNodes[nodeId]?.getName() || nodeId;

  return (
    <div>
      <EbiBreadcrumbsBar
        graph={graph}
        entries={breadcrumbs}
      />
      <main className="container mx-auto px-4 my-8">
        <EdgeMetadataDialog open={openEdgeId !== null} onClose={() => setOpenEdgeId(null)} graph={graph} edgeId={openEdgeId} />
        <div className="text-2xl font-bold my-6">
          Edge Search
          {totalResults > 0 && !loading && (
            <span className="text-base font-normal text-gray-500 ml-3">
              {totalResults.toLocaleString()} results
            </span>
          )}
        </div>

        {/* Active filters */}
        {hasFilters && (
          <div className="flex flex-wrap gap-2 mb-4">
            {typeFilters.map((t) => chip(TYPE, "Type", t, t, "bg-blue-100 text-blue-800"))}
            {dsFilters.map((ds) => chip(DATASOURCES, "Datasource", ds, ds, "bg-green-100 text-green-800"))}
            {fromNodeId && chip(FROM, "From", fromNodeId, endNodeName(fromNodeId), "bg-purple-100 text-purple-800")}
            {toNodeId && chip(TO, "To", toNodeId, endNodeName(toNodeId), "bg-purple-100 text-purple-800")}
          </div>
        )}

        <div className="flex gap-6">
          {/* Facets sidebar */}
          <div className="w-64 flex-shrink-0">
            <button
              className="text-sm text-gray-500 mb-2 flex items-center gap-1"
              onClick={() => setHideFilters(!hideFilters)}
            >
              <KeyboardArrowDown
                style={{ transform: hideFilters ? "rotate(-90deg)" : undefined, transition: "transform 0.2s" }}
                fontSize="small"
              />
              Filters
            </button>

            {!hideFilters && (
              <>
                {/* End nodes */}
                <div className="mb-4">
                  <div className="text-xs font-semibold text-gray-500 uppercase mb-1">From node</div>
                  <NodeSelectorBox
                    graph={graph}
                    placeholder="Any node"
                    selectedNode={fromNodeId ? endNodes[fromNodeId] || new GraphNodeRef({ "grebi:nodeId": fromNodeId }) : undefined}
                    onNodeSelect={(node) => setSingle(FROM, node.getNodeId())}
                    onClear={() => setSingle(FROM, null)}
                  />
                </div>
                <div className="mb-4">
                  <div className="text-xs font-semibold text-gray-500 uppercase mb-1">To node</div>
                  <NodeSelectorBox
                    graph={graph}
                    placeholder="Any node"
                    selectedNode={toNodeId ? endNodes[toNodeId] || new GraphNodeRef({ "grebi:nodeId": toNodeId }) : undefined}
                    onNodeSelect={(node) => setSingle(TO, node.getNodeId())}
                    onClear={() => setSingle(TO, null)}
                  />
                </div>

                {/* Type facets: any number, alternatives */}
                {Object.keys(typeFacets).length > 0 && (
                  <div className="mb-4">
                    <div className="text-xs font-semibold text-gray-500 uppercase mb-1">Edge Type</div>
                    <div className="max-h-60 overflow-y-auto">
                      {Object.entries(typeFacets)
                        .sort((a, b) => b[1] - a[1])
                        .map(([type, count]) => (
                          <button
                            key={type}
                            aria-pressed={typeFilters.includes(type)}
                            className={`block w-full text-left text-sm px-2 py-1 rounded hover:bg-gray-100 ${
                              typeFilters.includes(type) ? "bg-blue-50 text-blue-700 font-medium" : "text-gray-700"
                            }`}
                            onClick={() => toggleValue(TYPE, type)}
                          >
                            <span className="truncate">{type}</span>
                            <span className="text-gray-400 text-xs ml-1">({count.toLocaleString()})</span>
                          </button>
                        ))}
                    </div>
                  </div>
                )}

                {/* Datasource facets: any number, alternatives */}
                {Object.keys(dsFacets).length > 0 && (
                  <div className="mb-4">
                    <div className="text-xs font-semibold text-gray-500 uppercase mb-1">Datasource</div>
                    <div className="max-h-60 overflow-y-auto">
                      {Object.entries(dsFacets)
                        .sort((a, b) => b[1] - a[1])
                        .map(([ds, count]) => (
                          <button
                            key={ds}
                            aria-pressed={dsFilters.includes(ds)}
                            className={`block w-full text-left text-sm px-2 py-1 rounded hover:bg-gray-100 ${
                              dsFilters.includes(ds) ? "bg-green-50 text-green-700 font-medium" : "text-gray-700"
                            }`}
                            onClick={() => toggleValue(DATASOURCES, ds)}
                          >
                            <span className="truncate">{ds}</span>
                            <span className="text-gray-400 text-xs ml-1">({count.toLocaleString()})</span>
                          </button>
                        ))}
                    </div>
                  </div>
                )}
              </>
            )}
          </div>

          {/* Results */}
          <div className="flex-grow min-w-0">
            {loading ? (
              <LoadingOverlay message="Searching edges..." />
            ) : error ? (
              <ErrorMessage what="Edges" error={error} />
            ) : edges.length === 0 ? (
              <div className="text-gray-500 py-8 text-center">No edges found.</div>
            ) : (
              <>
                <table className="w-full text-sm border border-gray-200 rounded-lg overflow-hidden">
                  <thead>
                    <tr className="bg-gray-50 text-left text-gray-600 border-b border-gray-200">
                      <th className="py-2 px-3 font-medium">From</th>
                      <th className="py-2 px-3 font-medium">
                        <button
                          className="inline-flex items-center gap-1 hover:text-gray-900"
                          title={`Sorted by type, ${sortDir === "asc" ? "ascending" : "descending"}; click to reverse`}
                          onClick={() => setSingle("sortDir", sortDir === "asc" ? "desc" : "asc")}
                        >
                          Edge Type
                          {sortDir === "asc" ? <ArrowUpward fontSize="inherit" /> : <ArrowDownward fontSize="inherit" />}
                        </button>
                      </th>
                      <th className="py-2 px-3 font-medium">To</th>
                      <th className="py-2 px-3 font-medium">Datasources</th>
                      <th className="py-2 px-3 font-medium"></th>
                    </tr>
                  </thead>
                  <tbody>
                    {edges.map((edge, i) => {
                      const from = edge.props["from"] ? edge.getFrom() : null;
                      const to = edge.props["to"] ? edge.getTo() : null;
                      return (
                        <tr
                          key={edge.getEdgeId() || i}
                          className={`border-b border-gray-100 ${i % 2 === 1 ? "bg-gray-50" : ""}`}
                        >
                          <td className="py-2 px-3">
                            {from ? (
                              <NodeRefLink graph={graph} nodeRef={from} showTypeChip={true} />
                            ) : (
                              <span className="text-gray-400 font-mono text-xs">
                                {edge.props["grebi:fromNodeId"]}
                              </span>
                            )}
                          </td>
                          <td className="py-2 px-3 font-mono text-gray-700 whitespace-nowrap">
                            {edge.getType()}
                          </td>
                          <td className="py-2 px-3">
                            {to ? (
                              <NodeRefLink graph={graph} nodeRef={to} showTypeChip={true} />
                            ) : (
                              <span className="text-gray-400 font-mono text-xs">
                                {edge.props["grebi:toNodeId"]}
                              </span>
                            )}
                          </td>
                          <td className="py-2 px-3">
                            <DatasourceTags dss={edge.getDatasources()} linked />
                          </td>
                          <td className="py-2 px-3 text-center">
                            {edge.getEdgeId() && (
                              <button
                                className="text-link-default hover:text-link-dark"
                                title="View edge properties"
                                aria-label={`View edge ${edge.getEdgeId()}`}
                                onClick={() => setOpenEdgeId(edge.getEdgeId())}
                              >
                                <Info fontSize="small" />
                              </button>
                            )}
                          </td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>

                {totalPages > 1 && (
                  <div className="flex justify-center mt-4">
                    <Pagination
                      count={totalPages}
                      page={page + 1}
                      onChange={(_, p) => setPage(p - 1)}
                      shape="rounded"
                      size="small"
                    />
                  </div>
                )}
              </>
            )}
          </div>
        </div>
      </main>
    </div>
  );
}
