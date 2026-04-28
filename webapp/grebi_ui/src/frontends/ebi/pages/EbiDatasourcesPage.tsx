import { useEffect, useState, Fragment } from "react";
import { Link } from "react-router-dom";
import React from "react";
import { Skeleton } from "@mui/material";
import { get } from "../../../app/api";
import EbiBreadcrumbsBar from "../EbiBreadcrumbsBar";

interface DatasourceConfig {
  id: string;
  description?: string;
  graph_properties?: Record<string, any>;
  [key: string]: any;
}

interface GraphMeta {
  subgraph_config?: {
    id?: string;
    name?: string;
    description?: string;
    datasource_configs?: DatasourceConfig[];
  };
}

export default function EbiDatasourcesPage() {
  document.title = "Graphs - GrEBI";

  let [graphs, setGraphs] = useState<string[] | null>(null);
  let [metaByGraph, setMetaByGraph] = useState<Record<string, GraphMeta>>({});
  let [globalStats, setGlobalStats] = useState<any | null>(null);
  const graphMetadataLoaded = graphs !== null && Object.keys(metaByGraph).length === graphs.length;

  useEffect(() => {
    get<any>("api/v1/stats").then(setGlobalStats);
    get<string[]>("api/v1/graphs").then((sgs) => {
      setGraphs(sgs);
      Promise.all(
        sgs.map((sg) =>
          get<GraphMeta>(`api/v1/graphs/${sg}`)
            .then((meta) => [sg, meta] as const)
            .catch(() => [sg, {}] as const)
        )
      ).then((entries) => {
        setMetaByGraph(Object.fromEntries(entries));
      });
    });
  }, []);

  if (!graphs) {
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
          {!graphMetadataLoaded &&
            graphs.map((sg) => (
              <div
                key={sg}
                className="block border border-gray-200 rounded-lg bg-white shadow-sm overflow-hidden"
              >
                <div className="px-5 py-4 border-b border-gray-100 flex items-center justify-between flex-wrap gap-2">
                  <div className="w-full">
                    <Skeleton variant="text" width={260} height={32} />
                    <Skeleton variant="text" width={360} />
                  </div>
                </div>
                <div className="px-5 py-3">
                  <Skeleton variant="text" width="90%" />
                  <Skeleton variant="text" width="70%" />
                </div>
              </div>
            ))}
          {graphMetadataLoaded &&
            graphs.map((sg) => {
              const meta = metaByGraph[sg];
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
