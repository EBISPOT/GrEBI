import React, { useEffect, useState, useRef } from "react";
import { getPaginated } from "../../app/api";
import { difference } from "../../app/util";
import GraphEdge from "../../model/GraphEdge";
import GraphNodeRef from "../../model/GraphNodeRef";
import { DatasourceTags } from "../DatasourceTag";
import LoadingOverlay from "../LoadingOverlay";

export interface ChainSegment {
  label: string;
  edgeType: string;
  direction: "incoming" | "outgoing";
}

/**
 * Inline panel for choosing a node to expand.
 * Left side: simple scrollable list of node names + datasource tags.
 * Right side: mini chain preview showing the path so far + pending dashed node.
 */
export default function EdgeExpandPanel({
  onSelectNode,
  onCancel,
  subgraph,
  nodeId,
  encodedNodeId,
  direction,
  edgeType,
  chain,
  rootLabel,
  dsExclude,
}: {
  onSelectNode: (node: GraphNodeRef) => void;
  onCancel: () => void;
  subgraph: string;
  nodeId: string;
  encodedNodeId: string;
  direction: "incoming" | "outgoing";
  edgeType: string;
  chain: ChainSegment[];
  rootLabel: string;
  dsExclude?: Set<string>;
}) {
  const [loading, setLoading] = useState(true);
  const [edges, setEdges] = useState<GraphEdge[]>([]);
  const [total, setTotal] = useState(0);
  const [page, setPage] = useState(0);
  const pageSize = 50;

  const canvasRef = useRef<HTMLCanvasElement>(null);

  useEffect(() => {
    async function fetchEdges() {
      setLoading(true);
      try {
        const endpoint =
          direction === "incoming" ? "incoming_edge_refs" : "outgoing_edge_refs";
        const paramEntries: [string, string][] = [
          ["page", String(page)],
          ["size", String(pageSize)],
          ["grebi:type", edgeType],
        ];
        // Filter out excluded datasources
        if (dsExclude) {
          for (const ds of dsExclude) {
            paramEntries.push(["-grebi:datasources", ds]);
          }
        }
        const params = new URLSearchParams(paramEntries);

        const res = (
          await getPaginated<any>(
            `api/v1/subgraphs/${subgraph}/nodes/${encodedNodeId}/${endpoint}?${params}`
          )
        ).map((e) => new GraphEdge(e));

        setEdges(res.elements);
        setTotal(res.totalElements);
      } catch (e) {
        console.error("Failed to load edges for expand panel", e);
      } finally {
        setLoading(false);
      }
    }

    fetchEdges();
  }, [direction, edgeType, encodedNodeId, subgraph, page, dsExclude]);

  // Reset page when key params change
  useEffect(() => {
    setPage(0);
  }, [edgeType, direction]);

  const handleSelect = (row: GraphEdge) => {
    const selectedNode =
      direction === "incoming" ? row.getFrom() : row.getTo();
    onSelectNode(selectedNode);
  };

  // Draw the chain preview on canvas
  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas) return;
    const ctx = canvas.getContext("2d");
    if (!ctx) return;

    const dpr = window.devicePixelRatio || 1;
    const w = canvas.clientWidth;
    const h = canvas.clientHeight;
    canvas.width = w * dpr;
    canvas.height = h * dpr;
    ctx.scale(dpr, dpr);
    ctx.clearRect(0, 0, w, h);

    // Chain: root → seg1 → seg2 → ... → pending(?)
    const allNodes: { label: string; pending?: boolean }[] = [{ label: rootLabel }];
    const allEdges: { label: string }[] = [];
    for (const seg of chain) {
      allEdges.push({ label: seg.edgeType });
      allNodes.push({ label: seg.label });
    }
    // Add the pending edge + node
    allEdges.push({ label: edgeType });
    allNodes.push({ label: "?", pending: true });

    const nodeCount = allNodes.length;
    if (nodeCount === 0) return;

    const nodeRadius = 24;
    const padding = 40;
    const totalWidth = w - padding * 2;
    const spacing = nodeCount > 1 ? totalWidth / (nodeCount - 1) : 0;
    const cy = h / 2;

    // Draw edges (lines + labels)
    for (let i = 0; i < allEdges.length; i++) {
      const x1 = padding + i * spacing + nodeRadius;
      const x2 = padding + (i + 1) * spacing - nodeRadius;

      // Line
      ctx.beginPath();
      ctx.moveTo(x1, cy);
      ctx.lineTo(x2, cy);
      const isPending = i === allEdges.length - 1;
      if (isPending) {
        ctx.setLineDash([4, 3]);
        ctx.strokeStyle = "#bbb";
      } else {
        ctx.setLineDash([]);
        ctx.strokeStyle = "#999";
      }
      ctx.lineWidth = 1.5;
      ctx.stroke();
      ctx.setLineDash([]);

      // Arrow head
      const arrowSize = 6;
      ctx.beginPath();
      ctx.moveTo(x2, cy);
      ctx.lineTo(x2 - arrowSize, cy - arrowSize);
      ctx.moveTo(x2, cy);
      ctx.lineTo(x2 - arrowSize, cy + arrowSize);
      ctx.strokeStyle = isPending ? "#bbb" : "#999";
      ctx.lineWidth = 1.5;
      ctx.stroke();

      // Edge label
      const midX = (x1 + x2) / 2;
      ctx.font = "10px 'Inter', sans-serif";
      ctx.fillStyle = "#888";
      ctx.textAlign = "center";
      ctx.textBaseline = "bottom";
      const edgeLabel = allEdges[i].label.length > 18
        ? allEdges[i].label.slice(0, 16) + "\u2026"
        : allEdges[i].label;
      ctx.fillText(edgeLabel, midX, cy - 6);
    }

    // Draw nodes
    for (let i = 0; i < allNodes.length; i++) {
      const cx2 = padding + i * spacing;
      const node = allNodes[i];

      ctx.beginPath();
      ctx.arc(cx2, cy, nodeRadius, 0, Math.PI * 2);

      if (node.pending) {
        // Hollow dashed circle
        ctx.setLineDash([5, 4]);
        ctx.strokeStyle = "#bbb";
        ctx.lineWidth = 2;
        ctx.stroke();
        ctx.setLineDash([]);
        // Question mark
        ctx.font = "bold 16px 'Inter', sans-serif";
        ctx.fillStyle = "#ccc";
        ctx.textAlign = "center";
        ctx.textBaseline = "middle";
        ctx.fillText("?", cx2, cy);
      } else {
        // Filled circle
        ctx.fillStyle = i === 0 ? "#1976d2" : "#66bb6a";
        ctx.fill();
        // Label inside
        ctx.font = "bold 10px 'Inter', sans-serif";
        ctx.fillStyle = "#fff";
        ctx.textAlign = "center";
        ctx.textBaseline = "middle";
        let label = node.label;
        const maxW = nodeRadius * 1.6;
        if (ctx.measureText(label).width > maxW) {
          while (label.length > 1 && ctx.measureText(label + "\u2026").width > maxW) {
            label = label.slice(0, -1);
          }
          label += "\u2026";
        }
        ctx.fillText(label, cx2, cy);
      }
    }
  }, [chain, rootLabel, edgeType, direction]);

  return (
    <div style={{ display: "flex", flex: 1, overflow: "hidden" }}>
      {/* Left: node list */}
      <div style={{
        flex: "0 0 60%",
        display: "flex",
        flexDirection: "column",
        borderRight: "1px solid #e0e0e0",
        minHeight: 0,
      }}>
        {/* Scrollable node list */}
        <div style={{
          flex: 1,
          overflow: "auto",
          padding: "4px 0",
        }}>
          {loading && <LoadingOverlay message="Loading..." />}
          {edges.map((edge, i) => {
            const nodeRef = direction === "incoming" ? edge.getFrom() : edge.getTo();
            return (
              <div
                key={i}
                onClick={() => handleSelect(edge)}
                style={{
                  padding: "8px 14px",
                  cursor: "pointer",
                  display: "flex",
                  alignItems: "center",
                  gap: "8px",
                  borderBottom: "1px solid #f0f0f0",
                  transition: "background 0.1s",
                }}
                onMouseEnter={(e) => (e.currentTarget.style.background = "#f0f5ff")}
                onMouseLeave={(e) => (e.currentTarget.style.background = "transparent")}
              >
                <span style={{ fontWeight: 500, fontSize: "13px", flex: 1, minWidth: 0, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                  {nodeRef.getName()}
                </span>
                <DatasourceTags dss={edge.getDatasources()} />
              </div>
            );
          })}
          {!loading && edges.length === 0 && (
            <div style={{ padding: "20px", textAlign: "center", color: "#999" }}>
              No nodes found
            </div>
          )}
        </div>
        {/* Sticky pagination at bottom */}
        {!loading && total > pageSize && (
          <div style={{
            display: "flex",
            justifyContent: "center",
            gap: "8px",
            padding: "8px",
            borderTop: "1px solid #e0e0e0",
            background: "#fafafa",
            flexShrink: 0,
          }}>
            <button
              disabled={page === 0}
              onClick={() => setPage(p => p - 1)}
              style={{ padding: "4px 12px", border: "1px solid #ccc", borderRadius: "4px", background: page === 0 ? "#f5f5f5" : "#fff", cursor: page === 0 ? "default" : "pointer" }}
            >
              Prev
            </button>
            <span style={{ fontSize: "12px", color: "#888", alignSelf: "center" }}>
              {page * pageSize + 1}\u2013{Math.min((page + 1) * pageSize, total)} of {total}
            </span>
            <button
              disabled={(page + 1) * pageSize >= total}
              onClick={() => setPage(p => p + 1)}
              style={{ padding: "4px 12px", border: "1px solid #ccc", borderRadius: "4px", background: (page + 1) * pageSize >= total ? "#f5f5f5" : "#fff", cursor: (page + 1) * pageSize >= total ? "default" : "pointer" }}
            >
              Next
            </button>
          </div>
        )}
      </div>
      {/* Right: chain preview */}
      <div style={{ flex: "0 0 40%", minWidth: 0, display: "flex", alignItems: "center", justifyContent: "center", background: "#fafafa" }}>
        <canvas
          ref={canvasRef}
          style={{ width: "100%", height: "100%" }}
        />
      </div>
    </div>
  );
}
