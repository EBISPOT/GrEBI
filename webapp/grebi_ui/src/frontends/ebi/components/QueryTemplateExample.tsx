import { useState, useMemo, useEffect } from "react";
import { PlayArrow, WarningAmber } from "@mui/icons-material";
import TabbedSourceView from "../../../components/query/TabbedSourceView";
import query2code from "../../../../../query2code.mjs";

export default function QueryTemplateExample(props: {
  id?: string;
  graph?: string;
  params?: string;
}) {
  const queryId = props.id || "";
  const graph = props.graph || "";

  const initialParams: Record<string, string> = useMemo(() => {
    if (!props.params) return {};
    try {
      return JSON.parse(props.params);
    } catch {
      return {};
    }
  }, [props.params]);

  const [params, setParams] = useState<Record<string, string>>(initialParams);
  const [response, setResponse] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [graphAvailable, setGraphAvailable] = useState<boolean | null>(null);

  // Check if the required graph is available
  useEffect(() => {
    if (!graph) return;
    const apiBase = (process.env.REACT_APP_APIURL || "").replace(/\/+$/, "");
    fetch(`${apiBase}/api/v1/graphs`)
      .then((r) => r.json())
      .then((graphs: string[]) => {
        setGraphAvailable(graphs.includes(graph));
      })
      .catch(() => setGraphAvailable(false));
  }, [graph]);

  const apiUrl = useMemo(() => {
    return `api/v1/graphs/${graph}/query/${queryId}`;
  }, [graph, queryId]);

  const codeTabs = useMemo(() => {
    const instanceUrl = (process.env.REACT_APP_APIURL || "").replace(/\/+$/, "");
    const paramIds = Object.keys(params);
    const snippets = query2code(instanceUrl, graph, queryId, paramIds, params);
    return Object.keys(snippets).map((title) => ({
      title,
      source: snippets[title].source,
      lang: snippets[title].lang,
    }));
  }, [graph, queryId, params]);

  const run = async () => {
    setLoading(true);
    setError(null);
    setResponse(null);
    try {
      const apiBase = (process.env.REACT_APP_APIURL || "").replace(/\/+$/, "");
      const qs = new URLSearchParams(params).toString();
      const fullUrl = `${apiBase}/${apiUrl}${qs ? "?" + qs : ""}`;
      const res = await fetch(fullUrl);
      const json = await res.json();
      const text = JSON.stringify(json, null, 2);
      setResponse(text.length > 2000 ? text.slice(0, 2000) + "\n... (truncated)" : text);
    } catch (e: any) {
      setError(e.message || "Request failed");
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="my-4 border border-gray-300 rounded-lg overflow-hidden">
      {/* Header */}
      <div className="bg-gray-100 px-4 py-2 flex items-center gap-2 border-b border-gray-300">
        <span className="text-xs font-bold uppercase px-2 py-0.5 rounded bg-purple-600 text-white">
          Query
        </span>
        <code className="text-sm font-mono text-gray-700 flex-1">{queryId}</code>
        <span className="text-xs text-gray-500">graph: {graph}</span>
        {graphAvailable === false && (
          <span className="flex items-center gap-1 text-xs text-amber-600">
            <WarningAmber fontSize="small" />
            Graph not loaded
          </span>
        )}
        <button
          className="flex items-center gap-1 px-3 py-1 text-xs rounded bg-purple-600 text-white hover:bg-purple-700 disabled:opacity-50"
          onClick={run}
          disabled={loading || graphAvailable === false}
        >
          <PlayArrow fontSize="small" />
          {loading ? "Running…" : "Try it"}
        </button>
      </div>

      {/* Parameters */}
      {Object.keys(params).length > 0 && (
        <div className="px-4 py-2 bg-white border-b border-gray-200">
          <div className="text-xs font-semibold text-gray-500 mb-1">Parameters</div>
          {Object.entries(params).map(([key, value]) => (
            <div key={key} className="flex items-center gap-2 mb-1">
              <label className="text-sm font-mono text-gray-600 w-32">{key}</label>
              <input
                className="border border-gray-300 rounded px-2 py-1 text-sm font-mono flex-1"
                value={value}
                onChange={(e) =>
                  setParams((prev) => ({ ...prev, [key]: e.target.value }))
                }
              />
            </div>
          ))}
        </div>
      )}

      {/* Code snippets */}
      <TabbedSourceView tabs={codeTabs} />

      {/* Response */}
      {(response || error) && (
        <div className="px-4 py-2 bg-gray-50 border-t border-gray-200">
          <div className="text-xs font-semibold text-gray-500 mb-1">Response</div>
          {error ? (
            <pre className="text-sm text-red-600 font-mono">{error}</pre>
          ) : (
            <pre className="text-sm font-mono overflow-x-auto max-h-96 overflow-y-auto whitespace-pre-wrap">
              {response}
            </pre>
          )}
        </div>
      )}
    </div>
  );
}
