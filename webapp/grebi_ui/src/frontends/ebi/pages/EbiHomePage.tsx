import { useEffect, useState, useCallback, useRef } from "react";
import { Link, useLocation, useNavigate, useParams } from "react-router-dom";
import React, { Fragment } from "react";
import { List, ListItem, MenuItem, Select, IconButton } from "@mui/material";
import { InfoOutlined } from "@mui/icons-material";
import { get, getPaginated } from "../../../app/api";
import SearchBox from "../../../components/SearchBox";
import SubgraphPicker from "../../../components/SubgraphPicker";
import CyclingQuestions from "../../../components/query/CyclingQuestions";
import urlJoin from "url-join";
import SourceCodeSection from "../../../components/query/SourceCodeSection";
import GraphView from "../../../components/node_graph_view/GraphView";
import GraphNode from "../../../model/GraphNode";

export default function EbiHomePage() {

  document.title = "EMBL-EBI Knowledge Graph";

  let params = useParams();
  let loc = useLocation();
  let navigate = useNavigate();

  let [stats, setStats] = useState<any|null>(null);
  let [subgraphs, setSubgraphs] = useState<string[]|null>(null);
  let [subgraphNames, setSubgraphNames] = useState<Record<string, string>>({});
  let [subgraph, setSubgraph] = useState<string|null>(params.subgraph || null);
  let [graphNode, setGraphNode] = useState<GraphNode|null>(null);
  let [graphLocked, setGraphLocked] = useState(false);
  let nodeCacheRef = useRef<Record<string, GraphNode>>({});

function navigateToSubgraph(sg: string) {
  let currentUrl = loc.pathname;
    setSubgraph(sg);
  if(currentUrl.indexOf("graphs/") !== -1) {
    let newUrl = currentUrl.replace(/graphs\/[^/]+/, `graphs/${sg}`);
    navigate(newUrl);
  } else {
    navigate(`/graphs/${sg}`);
  }
}


  useEffect(() => {
    get<Stats>("api/v1/stats").then(r => setStats(r));
  }, [subgraph]);

  const onExampleChange = useCallback((sourceId: string | null, title: string | null) => {
    if (!subgraph || !sourceId) return;
    const cached = nodeCacheRef.current[sourceId];
    if (cached) {
      setGraphNode(cached);
      return;
    }
    getPaginated<any>(`api/v1/subgraphs/${subgraph}/nodes`, { "grebi:sourceIds": sourceId, size: "1" })
      .then(r => {
        if (r.elements.length > 0) {
          const node = new GraphNode(r.elements[0]);
          nodeCacheRef.current[sourceId] = node;
          setGraphNode(node);
        }
      })
      .catch(() => {});
  }, [subgraph]);

  const onAllSourceIds = useCallback((sourceIds: string[]) => {
    if (!subgraph) return;
    for (const sid of sourceIds) {
      if (nodeCacheRef.current[sid]) continue;
      getPaginated<any>(`api/v1/subgraphs/${subgraph}/nodes`, { "grebi:sourceIds": sid, size: "1" })
        .then(r => {
          if (r.elements.length > 0) {
            nodeCacheRef.current[sid] = new GraphNode(r.elements[0]);
          }
        })
        .catch(() => {});
    }
  }, [subgraph]);

  const lockGraph = useCallback(() => {
    setGraphLocked(true);
  }, []);

  useEffect(() => {
    get<string[]>("api/v1/subgraphs").then(r => {
      setSubgraphs(r)

      if(!subgraph)
        setSubgraph(r[0])

      Promise.all(r.map(sg => get<any>(`api/v1/subgraphs/${sg}`).then(meta => {
        const name = meta.subgraph_config?.name || meta.subgraph_name || sg;
        return [sg, name] as [string, string];
      }).catch(() => [sg, sg] as [string, string])))
        .then(pairs => setSubgraphNames(Object.fromEntries(pairs)));
    });
  }, []);

  if(!subgraph) {
    return <div className="spinner-default w-7 h-7" />
  }

  return (
    <div>
      <main className="container mx-auto px-4 h-fit">
        <div className="grid grid-cols-2 lg:grid-cols-1 lg:gap-8">
          <div className="lg:col-span-3">
            <div className="bg-gradient-to-r from-neutral-light/50 to-white rounded-lg mt-8 mb-2 p-8 pb-4">
              <div className="mb-4">
                <div className="text-3xl text-neutral-black font-bold">
                  Welcome to GrEBI (Graphs@EBI)
                </div>
              </div>
              {subgraphs && subgraph ?
                <Fragment>
                  <SearchBox subgraph={subgraph} placeholder={`Search ${subgraphNames[subgraph] || subgraph} for knowledge about...`} />
                  <div className="flex gap-6 items-start mb-4 mt-4">
                    <div className="flex-grow min-w-0">
                      <p>
                        GrEBI enables researchers and their LLM agents to search and explore biomedical knowledge graphs derived from <Link className="link-default" to="https://www.ebi.ac.uk/services/">EMBL-EBI databases</Link>, the <Link className="link-default" to="https://monarchinitiative.org/">MONARCH Initiative</Link>, <Link className="link-default" to="https://dismech.monarchinitiative.org/">DisMech</Link>, <Link className="link-default" to="https://robokop.renci.org/api-docs/docs/automat/robokop-kg">ROBOKOP</Link>, <Link className="link-default" to="https://www.ebi.ac.uk/ols4">OLS</Link>, <Link className="link-default" to="https://github.com/INCATools/ubergraph">UberGraph</Link>, and many other sources.  For more information see the <Link className="link-default" to="https://github.com/EBISPOT/GrEBI">GrEBI GitHub repository</Link>.
                        <br/>
                        <br/>
                        MCP endpoint (Streamable HTTP): <code className="text-sm text-blue-600">https://wwwdev.ebi.ac.uk/kg/api/v1/mcp</code>
                      </p>
                    </div>
                    {subgraphs.length > 0 && (
                    <div className="flex-shrink-0">
                      <div className="text-lg font-semibold text-gray-700 mb-2">Select a graph to search</div>
                    <table className="text-sm border border-gray-200 rounded-lg overflow-hidden">
                      <thead>
                        <tr className="bg-gray-100 text-left text-gray-600 border-b border-gray-200">
                          <th className="py-2 px-3 font-medium">Graph</th>
                          <th className="py-2 px-3 font-medium">Name</th>
                          <th className="py-2 px-3 font-medium text-right">Nodes</th>
                          <th className="py-2 px-3 font-medium text-right">Edges</th>
                          <th className="py-2 px-3 w-8"></th>
                        </tr>
                      </thead>
                      <tbody>
                        {subgraphs.map((sg, i) => (
                          <tr
                            key={sg}
                            className={`hover:bg-blue-50 cursor-pointer transition-colors ${sg === subgraph ? 'bg-blue-50/50' : i % 2 === 1 ? 'bg-gray-50' : ''}`}
                            onClick={() => navigateToSubgraph(sg)}
                          >
                            <td className="py-2 px-3 font-mono text-gray-700 whitespace-nowrap">
                              <input
                                type="radio"
                                name="subgraph"
                                checked={sg === subgraph}
                                onChange={() => navigateToSubgraph(sg)}
                                className="mr-2"
                              />
                              {sg}
                            </td>
                            <td className="py-2 px-3 whitespace-nowrap">{subgraphNames[sg] || sg}</td>
                            <td className="py-2 px-3 text-right tabular-nums text-gray-600">
                              {stats && stats[sg] ? stats[sg].num_nodes.toLocaleString() : '—'}
                            </td>
                            <td className="py-2 px-3 text-right tabular-nums text-gray-600">
                              {stats && stats[sg] ? stats[sg].num_edges.toLocaleString() : '—'}
                            </td>
                            <td className="py-2 px-3">
                              <Link to={`/graphs/${sg}`} onClick={(e) => e.stopPropagation()}>
                                <IconButton size="small" title="Subgraph info">
                                  <InfoOutlined fontSize="small" />
                                </IconButton>
                              </Link>
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                    </div>
                    )}
                  </div>
                  <div className="flex items-center gap-12 mt-8 mb-4">
                    <a target="_blank" href="https://www.ebi.ac.uk/">
                      <img style={{width:'100px'}} src={urlJoin(process.env.PUBLIC_URL!, "/ebi.png")} alt="EMBL-EBI" />
                    </a>
                    <a target="_blank" href="https://monarchinitiative.org/">
                      <img style={{width:'100px'}} src={urlJoin(process.env.PUBLIC_URL!, "/monarch.png")} alt="MONARCH Initiative" />
                    </a>
                    <a target="_blank" href="https://mousephenotype.org/">
                      <img style={{width:'100px'}} src={urlJoin(process.env.PUBLIC_URL!, "/impc.svg")} alt="International Mouse Phenotyping Consortium (IMPC)" />
                    </a>
                  </div>
                </Fragment>
                :
                <div className="flex flex-nowrap gap-4">
                  Loading graphs...
                </div>
              }
            </div>
          </div>
          </div>

          {subgraph && (
            <div className="mt-2 mb-8">
              <CyclingQuestions subgraph={subgraph} autoPlay={!graphLocked} onExampleChange={onExampleChange} onAllSourceIds={onAllSourceIds} />
            </div>
          )}




{subgraph && graphNode && (
          <div className="mt-8">
            <div
              style={{ height: "600px", border: "1px solid #e0e0e0", borderRadius: "8px", overflow: "hidden", position: "relative" }}
              onMouseDown={lockGraph}
            >
              <GraphView subgraph={subgraph} node={graphNode} />
            </div>
          </div>
        )}

      </main>
    </div>
  );
}
