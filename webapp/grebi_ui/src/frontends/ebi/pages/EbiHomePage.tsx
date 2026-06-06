import { useEffect, useState, useCallback, useRef } from "react";
import { Link, useLocation, useNavigate, useParams } from "react-router-dom";
import React, { Fragment } from "react";
import { List, ListItem, MenuItem, Select, IconButton, Skeleton } from "@mui/material";
import { InfoOutlined } from "@mui/icons-material";
import { get, getPaginated } from "../../../app/api";
import SearchBox from "../../../components/SearchBox";
import GraphPicker from "../../../components/GraphPicker";
import CyclingQuestions from "../../../components/query/CyclingQuestions";
import urlJoin from "url-join";
import SourceCodeSection from "../../../components/query/SourceCodeSection";
import GraphView from "../../../components/node_graph_view/GraphView";
import { prefetchNodeEdgeCounts } from "../../../components/node_graph_view/edgeCountsCache";
import GraphNode from "../../../model/GraphNode";

function buildNodeCacheKey(graph: string, sourceId: string): string {
  return `${graph}::${sourceId}`;
}

export default function EbiHomePage() {

  document.title = "EMBL-EBI Knowledge Graph";

  let params = useParams();
  let loc = useLocation();
  let navigate = useNavigate();

  let [stats, setStats] = useState<any|null>(null);
  let [graphs, setGraphs] = useState<string[]|null>(null);
  let [graphNames, setGraphNames] = useState<Record<string, string>>({});
  let [graph, setGraph] = useState<string|null>(params.graph || null);
  let [graphNode, setGraphNode] = useState<GraphNode|null>(null);
  let [graphLocked, setGraphLocked] = useState(false);
  let [graphHovered, setGraphHovered] = useState(false);
  let nodeCacheRef = useRef<Record<string, GraphNode>>({});
  let nodeRequestRef = useRef<Record<string, Promise<GraphNode | null>>>({});
  let activeNodeKeyRef = useRef<string | null>(null);
  const graphMetadataLoaded = graphs !== null && Object.keys(graphNames).length === graphs.length;

function selectGraph(sg: string) {
  let currentUrl = loc.pathname;
  setGraph(sg);
  // On a graph-specific page, swap the graph in the URL. On the homepage
  // (no graph in the URL) just update the selected graph and stay put, so
  // the search box and examples target the chosen graph.
  if(currentUrl.indexOf("graphs/") !== -1) {
    let newUrl = currentUrl.replace(/graphs\/[^/]+/, `graphs/${sg}`);
    navigate(newUrl);
  }
}


  useEffect(() => {
    get<Stats>("api/v1/stats").then(r => setStats(r));
  }, [graph]);

  useEffect(() => {
    activeNodeKeyRef.current = null;
    setGraphNode(null);
  }, [graph]);

  const ensureNodeLoaded = useCallback((sourceId: string, makeActive = false) => {
    if (!graph) return Promise.resolve<GraphNode | null>(null);

    const cacheKey = buildNodeCacheKey(graph, sourceId);
    if (makeActive) {
      activeNodeKeyRef.current = cacheKey;
    }

    const cached = nodeCacheRef.current[cacheKey];
    if (cached) {
      prefetchNodeEdgeCounts(graph, cached.getEncodedNodeId());
      if (makeActive && activeNodeKeyRef.current === cacheKey) {
        setGraphNode(cached);
      }
      return Promise.resolve(cached);
    }

    const pending = nodeRequestRef.current[cacheKey];
    if (pending) {
      if (makeActive) {
        pending.then(node => {
          if (node && activeNodeKeyRef.current === cacheKey) {
            setGraphNode(node);
          }
        }).catch(() => {});
      }
      return pending;
    }

    const request = getPaginated<any>(`api/v1/graphs/${graph}/nodes`, {
      "grebi:sourceIds": sourceId,
      size: "1",
      resolve: "false",
    })
      .then(r => {
        if (r.elements.length === 0) return null;
        const node = new GraphNode(r.elements[0]);
        nodeCacheRef.current[cacheKey] = node;
        prefetchNodeEdgeCounts(graph, node.getEncodedNodeId());
        if (activeNodeKeyRef.current === cacheKey) {
          setGraphNode(node);
        }
        return node;
      })
      .catch(() => null)
      .finally(() => {
        delete nodeRequestRef.current[cacheKey];
      });

    nodeRequestRef.current[cacheKey] = request;
    return request;
  }, [graph]);

  const onVisibleSourceIdsChange = useCallback((currentSourceId: string | null, nextSourceId: string | null) => {
    if (!currentSourceId) {
      activeNodeKeyRef.current = null;
      setGraphNode(null);
      return;
    }

    void ensureNodeLoaded(currentSourceId, true);

    if (nextSourceId && nextSourceId !== currentSourceId) {
      void ensureNodeLoaded(nextSourceId, false);
    }
  }, [ensureNodeLoaded]);

  const lockGraph = useCallback(() => {
    setGraphLocked(true);
  }, []);

  useEffect(() => {
    get<string[]>("api/v1/graphs").then(r => {
      setGraphs(r)

      if(!graph)
        setGraph(r[0])

      Promise.all(r.map(sg => get<any>(`api/v1/graphs/${sg}`).then(meta => {
        const name = meta.subgraph_config?.name || meta.subgraph_name || sg;
        return [sg, name] as [string, string];
      }).catch(() => [sg, sg] as [string, string])))
        .then(pairs => setGraphNames(Object.fromEntries(pairs)));
    });
  }, []);

  if(!graph) {
    return <div className="spinner-default w-7 h-7" />
  }

  return (
    <div>
      <main className="container mx-auto px-4 h-fit">
        <section className="bg-gradient-to-r from-neutral-light/50 to-white rounded-lg mt-8 mb-2 p-8 pb-4">
          <div className="mb-4">
            <div className="text-3xl text-neutral-black font-bold">
              Welcome to GrEBI (Graphs@EBI)
            </div>
          </div>
          {graphs && graph ? (
            <Fragment>
              <SearchBox
                graph={graph}
                placeholder={`Search ${graphMetadataLoaded ? graphNames[graph] || graph : "this graph"} for knowledge about...`}
              />
              <div className="mt-4 grid grid-cols-1 2xl:grid-cols-[minmax(0,1fr)_minmax(0,38rem)] gap-6 items-start">
                <div className="min-w-0">
                  <p>
                    GrEBI enables researchers and their LLM agents to search and explore biomedical knowledge graphs derived from <Link className="link-default" to="https://www.ebi.ac.uk/services/">EMBL-EBI databases</Link>, the <Link className="link-default" to="https://monarchinitiative.org/">MONARCH Initiative</Link>, <Link className="link-default" to="https://dismech.monarchinitiative.org/">DisMech</Link>, <Link className="link-default" to="https://robokop.renci.org/api-docs/docs/automat/robokop-kg">ROBOKOP</Link>, <Link className="link-default" to="https://www.ebi.ac.uk/ols4">OLS</Link>, <Link className="link-default" to="https://github.com/INCATools/ubergraph">UberGraph</Link>, and many other sources. For more information see the <Link className="link-default" to="https://github.com/EBISPOT/GrEBI">GrEBI GitHub repository</Link>.
                    <br />
                    <br />
                    MCP endpoint (Streamable HTTP): <code className="text-sm text-blue-600">https://wwwdev.ebi.ac.uk/kg/api/v1/mcp</code>
                  </p>
                </div>
                {graphs.length > 0 && (
                  <div className="min-w-0">
                    <div className="text-lg font-semibold text-gray-700 mb-2">Select a graph to search</div>
                    <div className="rounded-lg border border-gray-200 bg-white shadow-sm overflow-hidden">
                      <div className="overflow-x-auto">
                        <table className="w-full table-fixed text-sm">
                          <colgroup>
                            <col style={{ width: "11rem" }} />
                            <col />
                            <col style={{ width: "6.5rem" }} />
                            <col style={{ width: "6.5rem" }} />
                            <col style={{ width: "3rem" }} />
                          </colgroup>
                          <thead>
                            <tr className="bg-gray-100 text-left text-gray-600 border-b border-gray-200">
                              <th className="py-2 px-3 font-medium">Graph</th>
                              <th className="py-2 px-3 font-medium">Name</th>
                              <th className="py-2 px-3 font-medium text-right">Nodes</th>
                              <th className="py-2 px-3 font-medium text-right">Edges</th>
                              <th className="py-2 px-3"></th>
                            </tr>
                          </thead>
                          <tbody>
                            {!graphMetadataLoaded &&
                              graphs.map((_, i) => (
                                <tr key={i} className={i % 2 === 1 ? "bg-gray-50" : ""}>
                                  <td className="py-2 px-3 align-top">
                                    <div className="flex items-center gap-2">
                                      <Skeleton variant="circular" width={16} height={16} />
                                      <Skeleton variant="text" width={120} />
                                    </div>
                                  </td>
                                  <td className="py-2 px-3 align-top">
                                    <Skeleton variant="text" width="90%" />
                                    <Skeleton variant="text" width="65%" />
                                  </td>
                                  <td className="py-2 px-3 text-right align-top">
                                    <div className="flex justify-end">
                                      <Skeleton variant="text" width={60} />
                                    </div>
                                  </td>
                                  <td className="py-2 px-3 text-right align-top">
                                    <div className="flex justify-end">
                                      <Skeleton variant="text" width={60} />
                                    </div>
                                  </td>
                                  <td className="py-2 px-3 align-top">
                                    <div className="flex justify-center">
                                      <Skeleton variant="circular" width={20} height={20} />
                                    </div>
                                  </td>
                                </tr>
                              ))}
                            {graphMetadataLoaded &&
                              graphs.map((sg, i) => (
                                <tr
                                  key={sg}
                                  className={`hover:bg-blue-50 cursor-pointer transition-colors ${sg === graph ? "bg-blue-50/50" : i % 2 === 1 ? "bg-gray-50" : ""}`}
                                  onClick={() => selectGraph(sg)}
                                >
                                  <td className="py-2 px-3 font-mono text-gray-700 align-top">
                                    <label className="flex items-start gap-2 min-w-0 cursor-pointer">
                                      <input
                                        type="radio"
                                        name="graph"
                                        checked={sg === graph}
                                        onChange={() => selectGraph(sg)}
                                        className="mt-0.5 shrink-0"
                                      />
                                      <span className="block break-all leading-snug" title={sg}>
                                        {sg}
                                      </span>
                                    </label>
                                  </td>
                                  <td className="py-2 px-3 align-top">
                                    <div className="whitespace-normal break-words leading-snug" title={graphNames[sg] || sg}>
                                      {graphNames[sg] || sg}
                                    </div>
                                  </td>
                                  <td className="py-2 px-3 text-right tabular-nums text-gray-600 align-top whitespace-nowrap">
                                    {stats && stats[sg] ? stats[sg].num_nodes.toLocaleString() : "—"}
                                  </td>
                                  <td className="py-2 px-3 text-right tabular-nums text-gray-600 align-top whitespace-nowrap">
                                    {stats && stats[sg] ? stats[sg].num_edges.toLocaleString() : "—"}
                                  </td>
                                  <td className="py-2 px-3 align-top">
                                    <Link to={`/graphs/${sg}`} onClick={(e) => e.stopPropagation()}>
                                      <IconButton size="small" title="Graph info">
                                        <InfoOutlined fontSize="small" />
                                      </IconButton>
                                    </Link>
                                  </td>
                                </tr>
                              ))}
                          </tbody>
                        </table>
                      </div>
                    </div>
                  </div>
                )}
              </div>
              <div className="flex items-center gap-12 mt-8 mb-4">
                <a target="_blank" href="https://www.ebi.ac.uk/">
                  <img style={{ width: "100px" }} src={urlJoin(process.env.PUBLIC_URL!, "/ebi.png")} alt="EMBL-EBI" />
                </a>
                <a target="_blank" href="https://monarchinitiative.org/">
                  <img style={{ width: "100px" }} src={urlJoin(process.env.PUBLIC_URL!, "/monarch.png")} alt="MONARCH Initiative" />
                </a>
                <a target="_blank" href="https://mousephenotype.org/">
                  <img style={{ width: "100px" }} src={urlJoin(process.env.PUBLIC_URL!, "/impc.svg")} alt="International Mouse Phenotyping Consortium (IMPC)" />
                </a>
              </div>
            </Fragment>
          ) : (
            <div className="flex flex-nowrap gap-4">Loading graphs...</div>
          )}
        </section>

        {graph && (
          <div className="mt-2 mb-8">
            <CyclingQuestions graph={graph} autoPlay={!graphLocked && !graphHovered} onVisibleSourceIdsChange={onVisibleSourceIdsChange} />
          </div>
        )}




{graph && graphNode && (
          <div className="mt-8">
            <div
              style={{ height: "600px", border: "1px solid #e0e0e0", borderRadius: "8px", overflow: "hidden", position: "relative" }}
              onMouseDown={lockGraph}
              onMouseEnter={() => setGraphHovered(true)}
              onMouseLeave={() => setGraphHovered(false)}
            >
              <GraphView graph={graph} node={graphNode} />
            </div>
          </div>
        )}

      </main>
    </div>
  );
}
