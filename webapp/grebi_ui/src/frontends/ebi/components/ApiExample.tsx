import { useState, useMemo } from "react";
import { PlayArrow } from "@mui/icons-material";
import TabbedSourceView from "../../../components/query/TabbedSourceView";
import api2code from "../../../../../api2code.mjs";

export default function ApiExample(props: {
  method?: string;
  url?: string;
  params?: string;
}) {
  const method = props.method || "GET";
  const baseUrl = props.url || "";

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

  const codeTabs = useMemo(() => {
    const instanceUrl = (process.env.REACT_APP_APIURL || "").replace(/\/+$/, "");
    const snippets = api2code(method, instanceUrl, baseUrl, params);
    return Object.keys(snippets).map((title) => ({
      title,
      source: snippets[title].source,
      lang: snippets[title].lang,
    }));
  }, [method, baseUrl, params]);

  const run = async () => {
    setLoading(true);
    setError(null);
    setResponse(null);
    try {
      const apiBase = (process.env.REACT_APP_APIURL || "").replace(/\/+$/, "");
      const qs = new URLSearchParams(params).toString();
      const fullUrl = `${apiBase}${baseUrl}${qs ? "?" + qs : ""}`;
      const res = await fetch(fullUrl);
      const json = await res.json();
      const text = JSON.stringify(json, null, 2);
      // Truncate for display
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
        <span className="text-xs font-bold uppercase px-2 py-0.5 rounded bg-blue-600 text-white">
          {method}
        </span>
        <code className="text-sm font-mono text-gray-700 flex-1">{baseUrl}</code>
        <button
          className="flex items-center gap-1 px-3 py-1 text-xs rounded bg-green-600 text-white hover:bg-green-700 disabled:opacity-50"
          onClick={run}
          disabled={loading}
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
