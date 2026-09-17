import { useEffect, useState } from "react";
import { Link } from "react-router-dom";
import { get } from "../app/api";
import encodeNodeId from "../encodeNodeId";
import GraphNodeRef from "../model/GraphNodeRef";
import PropVal from "../model/PropVal";
import Refs from "../model/Refs";
import PropList from "./PropList";
import LoadingOverlay from "./LoadingOverlay";
import ErrorMessage from "./ErrorMessage";
import { DatasourceTags } from "./DatasourceTag";

/** The page of an edge, which the edge dialog links to. */
export function edgePageUrl(graph: string, edgeId: string): string {
  return `/graphs/${graph}/edges/${encodeNodeId(edgeId)}`;
}

/**
 * An edge's endpoints, type, datasources and properties, loaded by id. Shared
 * by the edge dialog and the edge page.
 */
export default function EdgeDetails({ graph, edgeId, onLoaded }: { graph: string; edgeId: string; onLoaded?: (edge: any) => void }) {
  const [edge, setEdge] = useState<any>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<any>(null);

  useEffect(() => {
    let cancelled = false;
    async function fetchEdge() {
      setLoading(true);
      setError(null);
      try {
        const loaded = await get<any>(`api/v1/graphs/${graph}/edges/${encodeNodeId(edgeId)}`);
        if (!cancelled) {
          setEdge(loaded);
          if (onLoaded) onLoaded(loaded);
        }
      } catch (e: any) {
        console.error("Failed to resolve edge:", e);
        if (!cancelled) {
          setEdge(null);
          setError(e);
        }
      } finally {
        if (!cancelled) setLoading(false);
      }
    }
    fetchEdge();
    return () => { cancelled = true; };
  }, [edgeId, graph]);

  if (loading) {
    return <LoadingOverlay message="Loading edge details..." />;
  }
  if (error) {
    return <ErrorMessage what="The edge" error={error} />;
  }
  if (!edge) {
    return null;
  }

  const refs = edge._refs ? new Refs(edge._refs) : new Refs(null);
  const fromNode = edge.from ? new GraphNodeRef(edge.from) : null;
  const toNode = edge.to ? new GraphNodeRef(edge.to) : null;
  const edgeType = edge["grebi:type"];
  const datasources: string[] = Array.isArray(edge["grebi:datasources"]) ? edge["grebi:datasources"] : [];

  // every property except the structure PropList hides anyway
  const propsMap: { [key: string]: PropVal[] } = {};
  for (const key of Object.keys(edge)) {
    if (["_refs", "from", "to", "grebi:edgeId", "grebi:fromNodeId", "grebi:toNodeId", "grebi:nodeId"].includes(key)) {
      continue;
    }
    propsMap[key] = PropVal.arrFrom(edge[key]);
  }

  return (
    <div>
      {(fromNode || toNode || edgeType) && (
        <div className="mb-4 p-3 bg-grey-50 rounded-lg text-sm">
          {fromNode && (
            <span>
              <b>From:</b>{" "}
              <Link className="link-default" to={`/graphs/${graph}/nodes/${fromNode.getEncodedNodeId()}`}>
                {fromNode.getName()}
              </Link>
            </span>
          )}
          {edgeType && (
            <span className="mx-2">
              <b>&rarr;</b>{" "}
              <code className="bg-grey-default rounded-sm px-1">{edgeType}</code>
            </span>
          )}
          {toNode && (
            <span>
              <b>&rarr;</b>{" "}
              <Link className="link-default" to={`/graphs/${graph}/nodes/${toNode.getEncodedNodeId()}`}>
                {toNode.getName()}
              </Link>
            </span>
          )}
          {datasources.length > 0 && (
            <span className="ml-3">
              <DatasourceTags dss={datasources} linked />
            </span>
          )}
        </div>
      )}
      <PropList graph={graph} refs={refs} props={propsMap} />
    </div>
  );
}
