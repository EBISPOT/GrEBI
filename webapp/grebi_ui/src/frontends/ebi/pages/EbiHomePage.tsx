import { useEffect, useState } from "react";
import { Link, useLocation, useNavigate, useParams } from "react-router-dom";
import React, { Fragment } from "react";
import { MenuItem, Select } from "@mui/material";
import { get } from "../../../app/api";
import EbiHeader from "../EbiHeader";
import SearchBox from "../../../components/SearchBox";
import SubgraphPicker from "../../../components/SubgraphPicker";
import urlJoin from "url-join";

export default function EbiHomePage() {

  document.title = "EMBL-EBI Knowledge Graph";

  let params = useParams();
  let loc = useLocation();
  let navigate = useNavigate();

  let [stats, setStats] = useState<any|null>(null);
  let [subgraphs, setSubgraphs] = useState<string[]|null>(null);
  let [subgraph, setSubgraph] = useState<string|null>(params.subgraph || null);

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
                Welcome to the EMBL-EBI Knowledge Graph
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
          <div>
                <p className="mb-3">
                  This website enables you to search and explore data from multiple EBI resources, linked together using LLM-mediated knowledge graphs and ontologies via the <Link className="link-default" to="https://monarchinitiative.org/">MONARCH Initiative KG</Link>, <Link className="link-default" to="https://robokop.renci.org/api-docs/docs/automat/robokop-kg">ROBOKOP</Link>, <Link className="link-default" to="https://www.ebi.ac.uk/ols4">OLS</Link>, <Link className="link-default" to="https://github.com/INCATools/ubergraph">UberGraph</Link>, and many other datasources.
                </p>
                <p className="mb-3">
                  The EMBL-EBI KG is an early prototype. If you are interested in querying the KG and/or have a potential application please <Link className="link-default" to="mailto:jmcl@ebi.ac.uk">get in touch</Link>.
                </p>
                <p>
                  For source code and more information see the <Link className="link-default" to="https://github.com/EBISPOT/GrEBI">GrEBI (Graphs@EBI) GitHub repository</Link>.
                </p>
          </div>
<div className="flex justify-left items-center mt-8 gap-4">
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

      </main>
    </div>
  );
}
