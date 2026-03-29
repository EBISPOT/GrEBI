import { Dialog, DialogTitle, DialogContent, IconButton } from "@mui/material";
import { Close } from "@mui/icons-material";
import { useEffect, useState } from "react";
import { Link } from "react-router-dom";
import { get } from "../../app/api";
import encodeNodeId from "../../encodeNodeId";
import GraphNodeRef from "../../model/GraphNodeRef";
import PropVal from "../../model/PropVal";
import Refs from "../../model/Refs";
import PropList from "../PropList";
import LoadingOverlay from "../LoadingOverlay";

interface EdgeMetadataDialogProps {
  open: boolean;
  onClose: () => void;
  graph: string;
  edgeId: string | null;
}

export default function EdgeMetadataDialog({
  open,
  onClose,
  graph,
  edgeId,
}: EdgeMetadataDialogProps) {
  const [resolvedEdge, setResolvedEdge] = useState<any>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!open || !edgeId) {
      setResolvedEdge(null);
      setError(null);
      return;
    }

    async function fetchEdge() {
      setLoading(true);
      setError(null);
      try {
        const encodedId = encodeNodeId(edgeId!);
        const edge = await get<any>(
          `api/v1/graphs/${graph}/edges/${encodedId}`
        );
        setResolvedEdge(edge);
      } catch (e: any) {
        console.error("Failed to resolve edge:", e);
        setError(e.message || "Failed to load edge details");
      } finally {
        setLoading(false);
      }
    }

    fetchEdge();
  }, [open, edgeId, graph]);

  if (!open) return null;

  const data = resolvedEdge || null;
  const refs = data?._refs ? new Refs(data._refs) : new Refs(null);

  const fromNode = data?.from ? new GraphNodeRef(data.from) : null;
  const toNode = data?.to ? new GraphNodeRef(data.to) : null;
  const edgeType = data?.["grebi:type"];

  // Build props map from resolved edge, converting values to PropVal arrays
  let propsMap: { [key: string]: PropVal[] } | null = null;
  if (data) {
    propsMap = {};
    for (const key of Object.keys(data)) {
      if (
        key === "_refs" ||
        key === "from" ||
        key === "to" ||
        key === "grebi:edgeId" ||
        key === "grebi:fromNodeId" ||
        key === "grebi:toNodeId" ||
        key === "grebi:nodeId"
      ) {
        continue;
      }
      propsMap[key] = PropVal.arrFrom(data[key]);
    }
  }

  return (
    <Dialog open={open} onClose={onClose} maxWidth="md" fullWidth>
      <DialogTitle className="flex justify-between items-center">
        <span>Edge Properties</span>
        <IconButton onClick={onClose} size="small">
          <Close />
        </IconButton>
      </DialogTitle>
      <DialogContent dividers>
        {loading && <LoadingOverlay message="Loading edge details..." />}
        {error && !loading && (
          <div className="text-sm text-red-600">{error}</div>
        )}
        {!loading && !error && propsMap && (
          <div>
            {(fromNode || toNode || edgeType) && (
              <div className="mb-4 p-3 bg-grey-50 rounded-lg text-sm">
                {fromNode && (
                  <span>
                    <b>From:</b>{" "}
                    <Link
                      className="link-default"
                      to={`/graphs/${graph}/nodes/${fromNode.getEncodedNodeId()}`}
                    >
                      {fromNode.getName()}
                    </Link>
                  </span>
                )}
                {edgeType && (
                  <span className="mx-2">
                    <b>&rarr;</b>{" "}
                    <code className="bg-grey-default rounded-sm px-1">
                      {edgeType}
                    </code>
                  </span>
                )}
                {toNode && (
                  <span>
                    <b>&rarr;</b>{" "}
                    <Link
                      className="link-default"
                      to={`/graphs/${graph}/nodes/${toNode.getEncodedNodeId()}`}
                    >
                      {toNode.getName()}
                    </Link>
                  </span>
                )}
              </div>
            )}
            <PropList graph={graph} refs={refs} props={propsMap} />
          </div>
        )}
      </DialogContent>
    </Dialog>
  );
}
