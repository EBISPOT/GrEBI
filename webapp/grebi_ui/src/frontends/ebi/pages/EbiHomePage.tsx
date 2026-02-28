import { useEffect, useState } from "react";
import { Link, useLocation, useNavigate, useParams } from "react-router-dom";
import React, { Fragment } from "react";
import { List, ListItem, MenuItem, Select } from "@mui/material";
import { get, getPaginated } from "../../../app/api";
import EbiHeader from "../EbiHeader";
import SearchBox from "../../../components/SearchBox";
import SubgraphPicker from "../../../components/SubgraphPicker";
import urlJoin from "url-join";
import SourceCodeSection from "../../../components/query/SourceCodeSection";
import { Power } from "@mui/icons-material";
import GraphView from "../../../components/node_graph_view/GraphView";
import GraphNode from "../../../model/GraphNode";

export default function EbiHomePage() {

  document.title = "EMBL-EBI Knowledge Graph";

  let params = useParams();
  let loc = useLocation();
  let navigate = useNavigate();

  let [stats, setStats] = useState<any|null>(null);
  let [subgraphs, setSubgraphs] = useState<string[]|null>(null);
  let [subgraph, setSubgraph] = useState<string|null>(params.subgraph || null);
  let [bronchiectasisNode, setBronchiectasisNode] = useState<GraphNode|null>(null);

function navigateToSubgraph(sg: string) {
  let currentUrl = loc.pathname;
    setSubgraph(sg);
  if(currentUrl.indexOf("subgraphs") !== -1) {
    let newUrl = currentUrl.replace(/subgraphs\/[^/]+/, `subgraphs/${sg}`);
    navigate(newUrl);
  } 
}


  useEffect(() => {
    get<Stats>("api/v1/stats").then(r => setStats(r));
  }, [subgraph]);
  useEffect(() => {
    if (!subgraph) return;
    getPaginated<any>(`api/v1/subgraphs/${subgraph}/nodes`, { "grebi:sourceIds": "mondo:0004822", size: "1" })
      .then(r => {
        if (r.elements.length > 0) {
          setBronchiectasisNode(new GraphNode(r.elements[0]));
        }
      })
      .catch(() => {});
  }, [subgraph]);
  useEffect(() => {
    get<string[]>("api/v1/subgraphs").then(r => {
      setSubgraphs(r)

      if(!subgraph)
        setSubgraph(r[0])
    });
  }, []);

  if(!subgraph) {
    return <div className="spinner-default w-7 h-7" />
  }

  return (
    <div>
        {/* <EbiHeader subgraph={subgraph} section="home" showBreadcrumbsBar={true} breadcrumbs={[
        ]} /> */}
        <EbiHeader subgraph={subgraph} section="home" />
      <main className="container mx-auto px-4 h-fit">
        <div className="grid grid-cols-2 lg:grid-cols-1 lg:gap-8">
          <div className="lg:col-span-3">
            <div className="bg-gradient-to-r from-neutral-light to-white rounded-lg my-8 p-8">
              <div className="text-3xl mb-4 text-neutral-black font-bold">
                Welcome to the EMBL-EBI Ontology Graph
              </div>
              {subgraphs && subgraph ?
                <Fragment>
                  <div className="flex flex-nowrap gap-4 mb-4">
                    <SubgraphPicker
                      subgraph={subgraph}
                      setSubgraph={navigateToSubgraph}
                      compact={false}
                    />
                  </div>
                  <div className="flex flex-nowrap gap-4 mb-4">
                    <SearchBox subgraph={subgraph} />
                  </div>
                </Fragment>
                :
                <div className="flex flex-nowrap gap-4 mb-4">
                  Loading graphs...
                </div>
              }
              <div className="grid md:grid-cols-2 grid-cols-1 gap-2">
                <div className="text-neutral-black">
                  <span>
                    Examples:&nbsp;
                    <Link to={"/subgraphs/" + subgraph + "/search?q=diabetes"} className="link-default">
                      diabetes
                    </Link>
                    &#44;&nbsp;
                    <Link to={"/subgraphs/" + subgraph + "/search?q=BRCA1"} className="link-default">
                      BRCA1
                    </Link>
                  </span>
                </div>
                <div className="md:text-right">
         
              {stats && subgraph && stats[subgraph] ? (
                <div className="text-neutral-black text-sm">
                  {/* <div className="mb-2 text-sm italic">
                    Updated&nbsp;
                    {moment(stats.lastModified).format(
                      "D MMM YYYY ddd HH:mm(Z)"
                    )}
                  </div> */}
                  <p>
                      <i>{stats[subgraph].num_nodes.toLocaleString()} nodes</i><br/>
                      <i>{stats[subgraph].num_edges.toLocaleString()} edges</i>
                    </p>
                </div>
              ) : (
                stats !== undefined ? <div className="text-neutral-black text-sm"></div> : <div className="spinner-default w-7 h-7" />
              )}
                </div>
              </div>
            </div>
          </div>
          </div>

          <div className="grid grid-cols-2 gap-4">
          <div className="grid gap-4">
                <p>
                  This website enables LLM agents to search and explore data from multiple EBI resources, linked together using knowledge graphs and ontologies via the <Link className="link-default" to="https://monarchinitiative.org/">MONARCH Initiative KG</Link>, <Link className="link-default" to="https://robokop.renci.org/api-docs/docs/automat/robokop-kg">ROBOKOP</Link>, <Link className="link-default" to="https://www.ebi.ac.uk/ols4">OLS</Link>, <Link className="link-default" to="https://github.com/INCATools/ubergraph">UberGraph</Link>, and many other datasources.
                  <br/>
                  <br/>
                  For source code and more information see the <Link className="link-default" to="https://github.com/EBISPOT/GrEBI">GrEBI (Graphs@EBI) GitHub repository</Link>.
                </p>
<div className="flex justify-left items-center gap-4">
  <a target="_blank" href="https://www.ebi.ac.uk/">
    <img 
      style={{width:'100px'}}
      src={urlJoin(process.env.PUBLIC_URL!, "/ebi.png")}
      alt="EMBL-EBI" 
    />
  </a>
  <a target="_blank" href="https://monarchinitiative.org/">
    <img 
      style={{width:'100px'}}
      src={urlJoin(process.env.PUBLIC_URL!, "/monarch.png")}
      alt="MONARCH Initiative" 
    />
  </a>
  <a target="_blank" href="https://mousephenotype.org/">
    <img 
      style={{width:'100px'}}
      src={urlJoin(process.env.PUBLIC_URL!, "/impc.svg")}
      alt="International Mouse Phenotyping Consortium (IMPC)" 
    />
  </a>
</div>
            </div>
                  
          <div className="grid gap-4">

<p className="flex items-center text-lg font-bold mb-3">
  <Power className="mr-2 h-5 w-5" />
  MCP Endpoints
</p>


  <ul className="space-y-3 pl-2">
    <li className="flex items-center gap-3 p-3 bg-gray-50 rounded-xl shadow-sm">
      <span className="font-medium text-gray-700 w-28">Legacy</span>
      <code className="text-sm text-blue-600 break-all">
        https://wwwdev.ebi.ac.uk/kg/api/v1/mcp/sse
      </code>
    </li>
    <li className="flex items-center gap-3 p-3 bg-gray-50 rounded-xl shadow-sm">
      <span className="font-medium text-gray-700 w-28">Streamable HTTP</span>
      <code className="text-sm text-blue-600 break-all">
        https://wwwdev.ebi.ac.uk/kg/api/v1/mcp
      </code>
    </li>
  </ul>
          </div>
          </div>
{subgraph && bronchiectasisNode && (
          <div className="mt-8">
            <div className="text-xl font-bold mb-2 text-neutral-black">
              Example: Bronchiectasis
            </div>
            <div style={{ height: "600px", border: "1px solid #e0e0e0", borderRadius: "8px", overflow: "hidden", position: "relative" }}>
              <GraphView subgraph={subgraph} node={bronchiectasisNode} />
            </div>
          </div>
        )}

      </main>
    </div>
  );
}
