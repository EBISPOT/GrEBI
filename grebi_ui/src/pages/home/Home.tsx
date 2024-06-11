import moment from "moment";
import { useEffect, useState } from "react";
import { Link } from "react-router-dom";
import Header from "../../components/Header";
import React, { Fragment } from "react";
import SearchBox from "../../components/SearchBox";
import { get } from "../../app/api";
import Stats from "../../model/Stats";

export default function Home() {

  document.title = "EMBL-EBI Knowledge Graph";

  let [stats, setStats] = useState<Stats|null>(null);
  
  useEffect(() => {
    get<Stats>("api/v1/stats").then(r => setStats(r));
  }, []);

  return (
    <div>
      <Header section="home" />
      <main className="container mx-auto px-4 h-fit">
        <div className="grid grid-cols-2 lg:grid-cols-1 lg:gap-8">
          <div className="lg:col-span-3">
            <div className="bg-gradient-to-r from-neutral-light to-white rounded-lg my-8 p-8">
              <div className="text-3xl mb-4 text-neutral-black font-bold">
                Welcome to the EMBL-EBI Knowledge Graph
              </div>
              <div className="flex flex-nowrap gap-4 mb-4">
                <SearchBox />
              </div>
              <div className="grid md:grid-cols-2 grid-cols-1 gap-2">
                <div className="text-neutral-black">
                  <span>
                    Examples:&nbsp;
                    <Link to={"/search?q=diabetes"} className="link-default">
                      diabetes
                    </Link>
                    &#44;&nbsp;
                    <Link to={"/search?q=BRCA1"} className="link-default">
                      BRCA1
                    </Link>
                  </span>
                </div>
                <div className="md:text-right">
         
              {stats ? (
                <div className="text-neutral-black text-sm">
                  {/* <div className="mb-2 text-sm italic">
                    Updated&nbsp;
                    {moment(stats.lastModified).format(
                      "D MMM YYYY ddd HH:mm(Z)"
                    )}
                  </div> */}
                  <p>
                      <i>{stats.num_nodes.toLocaleString()} nodes</i><br/>
                      <i>{stats.num_edges.toLocaleString()} edges</i>
                    </p>
                </div>
              ) : (
                  <div className="spinner-default w-7 h-7" />
              )}
                </div>
              </div>
            </div>
          </div>
          </div>
          <div>
                <p className="mb-3">
                  This website enables you to search and explore data from <Link className="link-default" to="/datasources">multiple EBI resources</Link> and the <Link className="link-default" to="https://monarchinitiative.org/">MONARCH Initiative KG</Link>. It combines data exports from each resource with rich ontology relationships loaded from <Link className="link-default" to="https://www.ebi.ac.uk/ols4">OLS</Link> and <Link className="link-default" to="https://github.com/INCATools/ubergraph">UberGraph</Link>.
                </p>
                <p>
                  The KG is a very early work in progress. No querying interface is currently provided other than simple search/browsing functionality. If you are interested in querying the KG and/or have a potential application please <Link className="link-default" to="mailto:jmcl@ebi.ac.uk">get in touch</Link>.
                </p>
          </div>
      </main>
    </div>
  );
}
