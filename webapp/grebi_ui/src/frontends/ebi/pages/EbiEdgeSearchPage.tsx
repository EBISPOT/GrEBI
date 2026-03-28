import React, { Fragment, useCallback, useEffect, useState } from "react";
import { Link, useParams, useSearchParams } from "react-router-dom";
import { get, getPaginated } from "../../../app/api";
import { difference } from "../../../app/util";
import EbiBreadcrumbsBar from "../EbiBreadcrumbsBar";
import GraphEdge from "../../../model/GraphEdge";
import NodeRefLink from "../../../components/node_edge_list/NodeRefLink";
import { DatasourceTags } from "../../../components/DatasourceTag";
import LoadingOverlay from "../../../components/LoadingOverlay";
import { Pagination } from "@mui/material";
import { Close, KeyboardArrowDown } from "@mui/icons-material";

export default function EbiEdgeSearchPage() {
  let params = useParams();
  const subgraph: string = params.subgraph as string;

  const [searchParams, setSearchParams] = useSearchParams();

  let [loading, setLoading] = useState(true);
  let [edges, setEdges] = useState<GraphEdge[]>([]);
  let [totalResults, setTotalResults] = useState(0);
  let [page, setPage] = useState(0);
  let [rowsPerPage] = useState(20);
  let [facets, setFacets] = useState<any>({});

  // Read filters from URL
  const typeFilter = searchParams.get("grebi:type") || "";
  const dsFilter = searchParams.get("grebi:datasources") || "";

  // Facet state
  const typeFacets: Record<string, number> = facets?.["grebi:type"] || {};
  const dsFacets: Record<string, number> = facets?.["grebi:datasources"] || {};
  const [hideFilters, setHideFilters] = useState(false);

  const hasFilters = !!(typeFilter || dsFilter);

  // Fetch stats for sidebar facets (used when API skips expensive facet computation)
  const [statsFacets, setStatsFacets] = useState<any>({});
  useEffect(() => {
    get<any>(`api/v1/subgraphs/${subgraph}/stats`).then((stats) => {
      const f: any = {};
      if (stats.edge_counts_by_type) f["grebi:type"] = stats.edge_counts_by_type;
      if (stats.edge_counts_by_datasource) f["grebi:datasources"] = stats.edge_counts_by_datasource;
      setStatsFacets(f);
    }).catch(() => {});
  }, [subgraph]);

  useEffect(() => {
    setPage(0);
  }, [typeFilter, dsFilter]);

  useEffect(() => {
    async function fetchEdges() {
      setLoading(true);
      let params: string[][] = [
        ["page", String(page)],
        ["size", String(rowsPerPage)],
        ["sortBy", "grebi:type"],
        ["sortDir", "asc"],
      ];
      if (typeFilter) params.push(["grebi:type", typeFilter]);
      if (dsFilter) params.push(["grebi:datasources", dsFilter]);

      let res = await getPaginated<any>(
        `api/v1/subgraphs/${subgraph}/edges?${new URLSearchParams(params)}`
      );
      setEdges(res.elements.map((e: any) => new GraphEdge(e)));
      setTotalResults(res.totalElements);
      const apiFacets = res.facetFieldsToCounts || {};
      // Use API facets if returned, otherwise fall back to stats
      const hasFacetData = Object.keys(apiFacets).some(k => Object.keys(apiFacets[k] || {}).length > 0);
      setFacets(hasFacetData ? apiFacets : statsFacets);
      setLoading(false);
    }
    fetchEdges();
  }, [subgraph, typeFilter, dsFilter, page, rowsPerPage, statsFacets]);

  const setFilter = useCallback(
    (key: string, value: string) => {
      const next = new URLSearchParams(searchParams);
      if (value) {
        next.set(key, value);
      } else {
        next.delete(key);
      }
      setSearchParams(next);
    },
    [searchParams, setSearchParams]
  );

  const totalPages = Math.ceil(totalResults / rowsPerPage);

  const breadcrumbs = [
    { url: `/graphs`, label: "Graphs" },
    { url: `/graphs/${subgraph}/edges`, label: "Edges" },
  ];

  return (
    <div>
      <EbiBreadcrumbsBar
        subgraph={subgraph}
        entries={breadcrumbs}
      />
      <main className="container mx-auto px-4 my-8">
        <div className="text-2xl font-bold my-6">
          Edge Search
          {totalResults > 0 && !loading && (
            <span className="text-base font-normal text-gray-500 ml-3">
              {totalResults.toLocaleString()} results
            </span>
          )}
        </div>

        {/* Active filters */}
        {(typeFilter || dsFilter) && (
          <div className="flex flex-wrap gap-2 mb-4">
            {typeFilter && (
              <span className="inline-flex items-center gap-1 px-3 py-1 bg-blue-100 text-blue-800 rounded-full text-sm">
                Type: {typeFilter}
                <button onClick={() => setFilter("grebi:type", "")} className="hover:text-blue-600">
                  <Close fontSize="small" />
                </button>
              </span>
            )}
            {dsFilter && (
              <span className="inline-flex items-center gap-1 px-3 py-1 bg-green-100 text-green-800 rounded-full text-sm">
                Datasource: {dsFilter}
                <button onClick={() => setFilter("grebi:datasources", "")} className="hover:text-green-600">
                  <Close fontSize="small" />
                </button>
              </span>
            )}
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
                {/* Type facets */}
                {Object.keys(typeFacets).length > 0 && (
                  <div className="mb-4">
                    <div className="text-xs font-semibold text-gray-500 uppercase mb-1">Edge Type</div>
                    <div className="max-h-60 overflow-y-auto">
                      {Object.entries(typeFacets)
                        .sort((a, b) => b[1] - a[1])
                        .map(([type, count]) => (
                          <button
                            key={type}
                            className={`block w-full text-left text-sm px-2 py-1 rounded hover:bg-gray-100 ${
                              typeFilter === type ? "bg-blue-50 text-blue-700 font-medium" : "text-gray-700"
                            }`}
                            onClick={() => setFilter("grebi:type", typeFilter === type ? "" : type)}
                          >
                            <span className="truncate">{type}</span>
                            <span className="text-gray-400 text-xs ml-1">({count.toLocaleString()})</span>
                          </button>
                        ))}
                    </div>
                  </div>
                )}

                {/* Datasource facets */}
                {Object.keys(dsFacets).length > 0 && (
                  <div className="mb-4">
                    <div className="text-xs font-semibold text-gray-500 uppercase mb-1">Datasource</div>
                    <div className="max-h-60 overflow-y-auto">
                      {Object.entries(dsFacets)
                        .sort((a, b) => b[1] - a[1])
                        .map(([ds, count]) => (
                          <button
                            key={ds}
                            className={`block w-full text-left text-sm px-2 py-1 rounded hover:bg-gray-100 ${
                              dsFilter === ds ? "bg-green-50 text-green-700 font-medium" : "text-gray-700"
                            }`}
                            onClick={() => setFilter("grebi:datasources", dsFilter === ds ? "" : ds)}
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
            ) : edges.length === 0 ? (
              <div className="text-gray-500 py-8 text-center">No edges found.</div>
            ) : (
              <>
                <table className="w-full text-sm border border-gray-200 rounded-lg overflow-hidden">
                  <thead>
                    <tr className="bg-gray-50 text-left text-gray-600 border-b border-gray-200">
                      <th className="py-2 px-3 font-medium">From</th>
                      <th className="py-2 px-3 font-medium">Edge Type</th>
                      <th className="py-2 px-3 font-medium">To</th>
                      <th className="py-2 px-3 font-medium">Datasources</th>
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
                              <NodeRefLink subgraph={subgraph} nodeRef={from} showTypeChip={true} />
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
                              <NodeRefLink subgraph={subgraph} nodeRef={to} showTypeChip={true} />
                            ) : (
                              <span className="text-gray-400 font-mono text-xs">
                                {edge.props["grebi:toNodeId"]}
                              </span>
                            )}
                          </td>
                          <td className="py-2 px-3">
                            <DatasourceTags dss={edge.getDatasources()} />
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
