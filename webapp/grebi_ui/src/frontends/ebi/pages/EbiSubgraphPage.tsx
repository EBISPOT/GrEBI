import { useEffect, useState, Fragment, useCallback } from "react";
import { useParams, useNavigate } from "react-router-dom";
import React from "react";
import { get } from "../../../app/api";
import EbiBreadcrumbsBar from "../EbiBreadcrumbsBar";
import DistributionPieChart from "../DistributionPieChart";
import SearchBox from "../../../components/SearchBox";

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
    datasource_configs?: DatasourceConfig[];
    [key: string]: any;
  };
  [key: string]: any;
}

interface DistributionStats {
  node_counts_by_datasource: Record<string, number>;
  node_counts_by_type: Record<string, number>;
  edge_counts_by_datasource: Record<string, number>;
  edge_counts_by_type: Record<string, number>;
}

export default function EbiSubgraphPage() {
  let params = useParams();
  let subgraph = params.subgraph!;

  const navigate = useNavigate();
  let [meta, setMeta] = useState<SubgraphMeta | null>(null);
  let [stats, setStats] = useState<any | null>(null);
  let [distStats, setDistStats] = useState<DistributionStats | null>(null);
  let [activeTab, setActiveTab] = useState<"Datasources" | "Node Types" | "Edge Types">("Datasources");

  const navToSearch = useCallback((filterKey: string, filterValue: string) => {
    navigate(`/graphs/${subgraph}/search?q=*&${encodeURIComponent(filterKey)}=${encodeURIComponent(filterValue)}`);
  }, [navigate, subgraph]);

  const navToEdgeSearch = useCallback((filterKey: string, filterValue: string) => {
    navigate(`/graphs/${subgraph}/edges?${encodeURIComponent(filterKey)}=${encodeURIComponent(filterValue)}`);
  }, [navigate, subgraph]);

  document.title = `${subgraph} - GrEBI`;

  useEffect(() => {
    get<SubgraphMeta>(`api/v1/subgraphs/${subgraph}`).then(setMeta);
    get<any>("api/v1/stats").then(setStats);
    get<DistributionStats>(`api/v1/subgraphs/${subgraph}/stats`)
      .then(setDistStats)
      .catch(() => {});
  }, [subgraph]);

  if (!meta) {
    return (
      <Fragment>
        <EbiBreadcrumbsBar subgraph={subgraph} entries={[
          { url: `/graphs`, label: "Graphs" }
        ]} />
        <main className="container mx-auto px-4 my-8">
          <div className="spinner-default w-7 h-7" />
        </main>
      </Fragment>
    );
  }

  const config = meta.subgraph_config;
  const sgName = config?.name || subgraph;
  const datasources: DatasourceConfig[] = config?.datasource_configs || [];
  const sgStats = stats && stats[subgraph];

  return (
    <Fragment>
      <EbiBreadcrumbsBar subgraph={subgraph} entries={[
        { url: `/graphs`, label: "Graphs" }
      ]} />
      <main className="container mx-auto px-4 my-8">
        <div className="text-2xl font-bold my-6">{sgName}</div>
        <div className="mb-6">
          <SearchBox subgraph={subgraph} placeholder={`Search ${sgName}...`} showSuggestions={true} />
        </div>

        <div className="mb-6">
          <table className="text-sm">
            <tbody>
              <tr>
                <td className="pr-4 py-1 text-gray-500 font-medium">ID</td>
                <td className="py-1 font-mono">{config?.id || subgraph}</td>
              </tr>
              {sgStats && (
                <>
                  <tr>
                    <td className="pr-4 py-1 text-gray-500 font-medium">Nodes</td>
                    <td className="py-1 tabular-nums">{sgStats.num_nodes.toLocaleString()}</td>
                  </tr>
                  <tr>
                    <td className="pr-4 py-1 text-gray-500 font-medium">Edges</td>
                    <td className="py-1 tabular-nums">{sgStats.num_edges.toLocaleString()}</td>
                  </tr>
                </>
              )}
              <tr>
                <td className="pr-4 py-1 text-gray-500 font-medium">Datasources</td>
                <td className="py-1">{datasources.length}</td>
              </tr>
            </tbody>
          </table>
        </div>

        {/* Distribution charts */}
        <div className="mb-8 border border-gray-200 rounded-lg bg-white shadow-sm p-5">
          {distStats ? (
            <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
              <DistributionPieChart data={distStats.node_counts_by_datasource} title="Node Datasources" onSliceClick={(name) => navToSearch("grebi:datasources", name)} />
              <DistributionPieChart data={distStats.node_counts_by_type} title="Node Types" onSliceClick={(name) => navToSearch("grebi:type", name)} />
              <DistributionPieChart data={distStats.edge_counts_by_datasource} title="Edge Datasources" onSliceClick={(name) => navToEdgeSearch("grebi:datasources", name)} />
              <DistributionPieChart data={distStats.edge_counts_by_type} title="Edge Types" onSliceClick={(name) => navToEdgeSearch("grebi:type", name)} />
            </div>
          ) : (
            <div className="text-center py-8">
              <div className="spinner-default w-5 h-5 inline-block" />
            </div>
          )}
        </div>

        {/* Tabbed tables */}
        <div className="mb-8 border border-gray-200 rounded-lg bg-white shadow-sm">
          <div className="flex border-b border-gray-200">
            {(["Datasources", "Node Types", "Edge Types"] as const).map((tab) => (
              <button
                key={tab}
                className={`px-4 py-2 text-sm font-medium -mb-px ${
                  activeTab === tab
                    ? "border-b-2 border-blue-500 text-blue-600"
                    : "text-gray-500 hover:text-gray-700"
                }`}
                onClick={() => setActiveTab(tab)}
              >
                {tab}
              </button>
            ))}
          </div>
          <div className="p-5">
            {activeTab === "Datasources" && (
              <table className="w-full text-sm">
                <thead>
                  <tr className="text-left text-gray-600 border-b border-gray-200">
                    <th className="py-2 px-3 font-medium">ID</th>
                    <th className="py-2 px-3 font-medium">Description</th>
                  </tr>
                </thead>
                <tbody>
                  {datasources.map((ds, i) => (
                    <tr
                      key={ds.id}
                      className={`border-b border-gray-100 ${i % 2 === 1 ? "bg-gray-50" : ""}`}
                    >
                      <td className="py-2 px-3 font-mono text-gray-700 whitespace-nowrap">{ds.id}</td>
                      <td className="py-2 px-3 text-gray-600">{ds.description || "—"}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
            {activeTab === "Node Types" && distStats && (
              <table className="w-full text-sm">
                <thead>
                  <tr className="text-left text-gray-600 border-b border-gray-200">
                    <th className="py-2 px-3 font-medium">Type</th>
                    <th className="py-2 px-3 font-medium text-right">Count</th>
                  </tr>
                </thead>
                <tbody>
                  {Object.entries(distStats.node_counts_by_type)
                    .sort((a, b) => b[1] - a[1])
                    .map(([type, count], i) => (
                      <tr key={type} className={`border-b border-gray-100 ${i % 2 === 1 ? "bg-gray-50" : ""}`}>
                        <td className="py-2 px-3 font-mono"><button className="link-default text-left" onClick={() => navToSearch("grebi:type", type)}>{type}</button></td>
                        <td className="py-2 px-3 text-gray-600 text-right tabular-nums">{count.toLocaleString()}</td>
                      </tr>
                    ))}
                </tbody>
              </table>
            )}
            {activeTab === "Edge Types" && distStats && (
              <table className="w-full text-sm">
                <thead>
                  <tr className="text-left text-gray-600 border-b border-gray-200">
                    <th className="py-2 px-3 font-medium">Type</th>
                    <th className="py-2 px-3 font-medium text-right">Count</th>
                  </tr>
                </thead>
                <tbody>
                  {Object.entries(distStats.edge_counts_by_type)
                    .sort((a, b) => b[1] - a[1])
                    .map(([type, count], i) => (
                      <tr key={type} className={`border-b border-gray-100 ${i % 2 === 1 ? "bg-gray-50" : ""}`}>
                        <td className="py-2 px-3 font-mono"><button className="link-default text-left" onClick={() => navToEdgeSearch("grebi:type", type)}>{type}</button></td>
                        <td className="py-2 px-3 text-gray-600 text-right tabular-nums">{count.toLocaleString()}</td>
                      </tr>
                    ))}
                </tbody>
              </table>
            )}
            {(activeTab !== "Datasources") && !distStats && (
              <div className="text-center py-8">
                <div className="spinner-default w-5 h-5 inline-block" />
              </div>
            )}
          </div>
        </div>
      </main>
    </Fragment>
  );
}
