import { useEffect, useState, Fragment } from "react";
import { Link } from "react-router-dom";
import React from "react";
import { get } from "../../../app/api";
import EbiBreadcrumbsBar from "../EbiBreadcrumbsBar";

interface DatasourceConfig {
  id: string;
  description?: string;
  graph_properties?: Record<string, any>;
  [key: string]: any;
}

interface SubgraphMeta {
  subgraph_config?: {
    id?: string;
    name?: string;
    description?: string;
    datasource_configs?: DatasourceConfig[];
  };
}

export default function EbiDatasourcesPage() {
  document.title = "Graphs - GrEBI";

  let [subgraphs, setSubgraphs] = useState<string[] | null>(null);
  let [metaBySubgraph, setMetaBySubgraph] = useState<Record<string, SubgraphMeta>>({});
  let [globalStats, setGlobalStats] = useState<any | null>(null);

  useEffect(() => {
    get<any>("api/v1/stats").then(setGlobalStats);
    get<string[]>("api/v1/subgraphs").then((sgs) => {
      setSubgraphs(sgs);
      sgs.forEach((sg) => {
        get<SubgraphMeta>(`api/v1/subgraphs/${sg}`)
          .then((meta) => setMetaBySubgraph((prev) => ({ ...prev, [sg]: meta })))
          .catch(() => {});
      });
    });
  }, []);

  if (!subgraphs) {
    return (
      <Fragment>
        <EbiBreadcrumbsBar entries={[
          { url: `/graphs`, label: "Graphs" }
        ]} />
        <main className="container mx-auto px-4 my-8">
          <div className="spinner-default w-7 h-7" />
        </main>
      </Fragment>
    );
  }

  return (
    <Fragment>
      <EbiBreadcrumbsBar entries={[
        { url: `/graphs`, label: "Graphs" }
      ]} />
      <main className="container mx-auto px-4 my-8">
        <div className="text-2xl font-bold my-6">Graphs</div>

        <div className="grid gap-6">
          {subgraphs.map((sg) => {
            const meta = metaBySubgraph[sg];
            const sgName = meta?.subgraph_config?.name || sg;
            const sgDescription = meta?.subgraph_config?.description;
            const datasources = meta?.subgraph_config?.datasource_configs || [];
            const sgStats = globalStats && globalStats[sg];

            return (
              <Link
                key={sg}
                to={`/graphs/${sg}`}
                className="block border border-gray-200 rounded-lg bg-white shadow-sm overflow-hidden hover:shadow-md hover:border-gray-300 transition-all no-underline text-inherit"
              >
                {/* Card header */}
                <div className="px-5 py-4 border-b border-gray-100 flex items-center justify-between flex-wrap gap-2">
                  <div>
                    <span className="text-lg font-semibold text-blue-600">
                      {sgName}
                    </span>
                    <span className="ml-2 text-xs text-gray-400 font-mono">({sg})</span>
                    <div className="text-xs text-gray-500 mt-0.5">
                      {datasources.length} datasource{datasources.length !== 1 ? "s" : ""}
                      {sgStats && (
                        <span>
                          {" · "}
                          {sgStats.num_nodes.toLocaleString()} nodes{" · "}
                          {sgStats.num_edges.toLocaleString()} edges
                        </span>
                      )}
                    </div>
                  </div>
                </div>
                {sgDescription && (
                  <div className="px-5 py-3 text-sm text-gray-600">
                    {sgDescription}
                  </div>
                )}
              </Link>
            );
          })}
        </div>
      </main>
    </Fragment>
  );
}
