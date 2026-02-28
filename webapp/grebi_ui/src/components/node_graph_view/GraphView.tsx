import React, { useEffect, useState, useMemo, useCallback, useRef } from "react";
import GraphNode from "../../model/GraphNode";
import GraphNodeRef from "../../model/GraphNodeRef";
import useGraphViewState from "./useGraphViewState";
import { computeRadialLayout, LayoutNode } from "./graphLayout";
import GraphRenderer from "./GraphRenderer";
import GraphViewControls from "./GraphViewControls";
import EdgeExpandPanel, { ChainSegment } from "./EdgeExpandPanel";
import LoadingOverlay from "../LoadingOverlay";
import { expandedKey } from "./useGraphViewState";
import { ArrowForward as ArrowForwardIcon, Close as CloseIcon } from "@mui/icons-material";
import { IconButton } from "@mui/material";

interface ExpandDialogState {
  open: boolean;
  parentNodeId: string;
  parentEncodedNodeId: string;
  direction: "incoming" | "outgoing";
  edgeType: string;
}

export default function GraphView({
  subgraph,
  node,
}: {
  subgraph: string;
  node: GraphNode;
}) {
  const state = useGraphViewState(subgraph);
  const [highlightedDs, setHighlightedDs] = useState<string | null>(null);
  const [highlightedEdgeType, setHighlightedEdgeType] = useState<string | null>(null);

  // Debounce highlight changes to avoid rapid re-renders during fast mouse movement
  const highlightDsTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const highlightEtTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  const debouncedSetHighlightedDs = useCallback((ds: string | null) => {
    if (highlightDsTimerRef.current) clearTimeout(highlightDsTimerRef.current);
    highlightDsTimerRef.current = setTimeout(() => setHighlightedDs(ds), 30);
  }, []);

  const debouncedSetHighlightedEt = useCallback((et: string | null) => {
    if (highlightEtTimerRef.current) clearTimeout(highlightEtTimerRef.current);
    highlightEtTimerRef.current = setTimeout(() => setHighlightedEdgeType(et), 30);
  }, []);

  const [expandDialog, setExpandDialog] = useState<ExpandDialogState>({
    open: false,
    parentNodeId: "",
    parentEncodedNodeId: "",
    direction: "incoming",
    edgeType: "",
  });

  // Load edge counts when node changes
  useEffect(() => {
    state.loadEdgeCounts(node);
  }, [node.getNodeId()]);

  // Compute the radial layout from state
  const layout = useMemo(() => {
    if (!state.root) return { nodes: [], edges: [] };
    return computeRadialLayout(
      state.root.getNodeId(),
      state.root.getName(),
      state.root.getEncodedNodeId(),
      state.incomingAggregated,
      state.outgoingAggregated,
      state.expandedNodes,
      state.dsExclude,
      state.hiddenEdgeTypes,
      state.autoExpandedNodes,
    );
  }, [
    state.root,
    state.incomingAggregated,
    state.outgoingAggregated,
    state.expandedNodes,
    state.dsExclude,
    state.hiddenEdgeTypes,
    state.autoExpandedNodes,
  ]);

  // Keep a stable layout that only updates when nothing is loading,
  // so the graph doesn't show partial state while expansions load.
  const stableLayoutRef = useRef(layout);
  if (!state.anyExpansionLoading && layout.nodes.length > 0) {
    stableLayoutRef.current = layout;
  }
  const stableLayout = stableLayoutRef.current;

  // All edge types for the controls
  const allEdgeTypes = useMemo(() => {
    const types = new Set<string>();
    for (const edgeType of Object.keys(state.incomingEdgeCounts)) {
      types.add(edgeType);
    }
    for (const edgeType of Object.keys(state.outgoingEdgeCounts)) {
      types.add(edgeType);
    }
    return Array.from(types).sort();
  }, [state.incomingEdgeCounts, state.outgoingEdgeCounts]);

  const handleClickCountNode = useCallback(
    (parentNodeId: string, parentEncodedNodeId: string, direction: "incoming" | "outgoing", edgeType: string) => {
      // Collapse any existing expansion at this slot so the old nodes disappear while picking
      state.collapseEdge(parentNodeId, direction, edgeType);
      setExpandDialog({ open: true, parentNodeId, parentEncodedNodeId, direction, edgeType });
    },
    [state.collapseEdge]
  );

  const handleClickExpandedNode = useCallback(
    (parentNodeId: string, parentEncodedNodeId: string, direction: "incoming" | "outgoing", edgeType: string) => {
      const key = expandedKey(parentNodeId, direction, edgeType);
      const expanded = state.expandedNodes.get(key);
      if (!expanded) return;

      const childNodeId = expanded.node.getNodeId();
      const hasDescendants = Array.from(state.expandedNodes.keys()).some(k => k.startsWith(childNodeId + "::"));

      if (hasDescendants) {
        // Rewind: collapse all descendants, making this the leaf node
        state.collapseDescendants(childNodeId);
      } else {
        // Leaf node: open dialog to swap the selection
        state.collapseEdge(parentNodeId, direction, edgeType);
        setExpandDialog({ open: true, parentNodeId, parentEncodedNodeId, direction, edgeType });
      }
    },
    [state.expandedNodes, state.collapseEdge, state.collapseDescendants]
  );

  const handleClickAutoExpandedNode = useCallback(
    (parentNodeId: string, parentEncodedNodeId: string, direction: "incoming" | "outgoing", edgeType: string) => {
      // Auto-expanded nodes have count=1, so we know exactly which node it is.
      // Directly expand it (same as if the user picked it from the dialog).
      const autoKey = expandedKey(parentNodeId, direction, edgeType);
      const autoNode = state.autoExpandedNodes.get(autoKey);
      if (autoNode) {
        state.expandEdge(parentNodeId, direction, edgeType, autoNode);
      }
    },
    [state.autoExpandedNodes, state.expandEdge]
  );

  const handleDoubleClickExpandedNode = useCallback(
    (parentNodeId: string, direction: "incoming" | "outgoing", edgeType: string, nodeId: string) => {
      const key = expandedKey(parentNodeId, direction, edgeType);
      const expanded = state.expandedNodes.get(key);
      if (expanded) {
        state.loadEdgeCounts(expanded.node);
        return;
      }
      // Also handle double-click on auto-expanded nodes
      const autoNode = state.autoExpandedNodes.get(key);
      if (autoNode) {
        state.loadEdgeCounts(autoNode);
      }
    },
    [state.expandedNodes, state.autoExpandedNodes, state.loadEdgeCounts]
  );

  const handleClickRoot = useCallback(() => {
    // Reset the graph view back to the original node
    state.loadEdgeCounts(node);
  }, [node, state.loadEdgeCounts]);


  const handleSelectNodeFromDialog = useCallback(
    (selectedNode: GraphNodeRef) => {
      const { parentNodeId, direction, edgeType } = expandDialog;
      state.expandEdge(parentNodeId, direction, edgeType, selectedNode);
      setExpandDialog((prev) => ({ ...prev, open: false }));
    },
    [expandDialog, state.expandEdge]
  );

  const handleCloseDialog = useCallback(() => {
    setExpandDialog((prev) => ({ ...prev, open: false }));
  }, []);

  // Hovered node state (lifted from GraphRenderer for the path bar)
  const [hoveredNode, setHoveredNode] = useState<LayoutNode | null>(null);

  // Resolve the parentLabel for a given node (hovered or expand dialog)
  const resolveParentLabel = useCallback(
    (parentNodeId?: string) => {
      if (!state.root) return "";
      if (parentNodeId && parentNodeId !== state.root.getNodeId()) {
        for (const exp of state.expandedNodes.values()) {
          if (exp.node.getNodeId() === parentNodeId) {
            return exp.node.getName();
          }
        }
      }
      return state.root.getName();
    },
    [state.root, state.expandedNodes]
  );

  // Build the path bar content for a node (hovered or expand-dialog target)
  const renderPathBar = (
    nodeLabel: string | null,
    direction: "incoming" | "outgoing",
    edgeType: string,
    parentLabel: string,
    count?: number,
  ) => {
    const leftNode = direction === "incoming" ? nodeLabel : parentLabel;
    const rightNode = direction === "incoming" ? parentLabel : nodeLabel;
    return (
      <>
        <span style={{ fontWeight: 600 }}>{leftNode || `${count?.toLocaleString() ?? 0} nodes`}</span>
        <ArrowForwardIcon sx={{ fontSize: 14, color: "#999" }} />
        <span style={{ fontFamily: "monospace", fontSize: "12px", color: "#666", position: "relative", top: "2px" }}>{edgeType}</span>
        <ArrowForwardIcon sx={{ fontSize: 14, color: "#999" }} />
        <span style={{ fontWeight: 600 }}>{rightNode || `${count?.toLocaleString() ?? 0} nodes`}</span>
      </>
    );
  };

  // Build the chain of expanded nodes leading to the current expand target
  const buildChain = useCallback((): ChainSegment[] => {
    if (!state.root || !expandDialog.open) return [];
    const segments: ChainSegment[] = [];
    // Walk from root through expanded nodes to reach expandDialog.parentNodeId
    let currentId = state.root.getNodeId();
    const targetId = expandDialog.parentNodeId;
    if (currentId === targetId) return segments;

    // BFS/DFS through expandedNodes to find path from root to target
    const visited = new Set<string>();
    const queue: { nodeId: string; path: ChainSegment[] }[] = [{ nodeId: currentId, path: [] }];
    while (queue.length > 0) {
      const { nodeId: nid, path } = queue.shift()!;
      if (visited.has(nid)) continue;
      visited.add(nid);
      for (const [, exp] of state.expandedNodes) {
        if (exp.parentNodeId === nid) {
          const newPath = [...path, { label: exp.node.getName(), edgeType: exp.edgeType, direction: exp.direction }];
          if (exp.node.getNodeId() === targetId) {
            return newPath;
          }
          queue.push({ nodeId: exp.node.getNodeId(), path: newPath });
        }
      }
    }
    return segments;
  }, [state.root, state.expandedNodes, expandDialog.open, expandDialog.parentNodeId]);

  const rootLabel = state.root?.getName() || "";

  // Resolve the edge count for a given (parentNodeId, direction, edgeType)
  const resolveEdgeCount = useCallback(
    (parentNodeId: string, direction: "incoming" | "outgoing", edgeType: string): number | undefined => {
      if (!state.root) return undefined;
      // Check if this is for the root node
      const edgeCounts = parentNodeId === state.root.getNodeId()
        ? (direction === "incoming" ? state.incomingEdgeCounts : state.outgoingEdgeCounts)
        : (() => {
            // Look for expanded node's edge counts
            for (const [, exp] of state.expandedNodes) {
              if (exp.node.getNodeId() === parentNodeId) {
                return direction === "incoming" ? exp.incomingEdgeCounts : exp.outgoingEdgeCounts;
              }
            }
            return {} as Record<string, Record<string, number>>;
          })();
      const dsToCount = edgeCounts[edgeType];
      if (!dsToCount) return undefined;
      let total = 0;
      for (const ds of Object.keys(dsToCount)) {
        if (!state.dsExclude.has(ds)) {
          total += dsToCount[ds];
        }
      }
      return total;
    },
    [state.root, state.incomingEdgeCounts, state.outgoingEdgeCounts, state.expandedNodes, state.dsExclude]
  );

  if (state.error) {
    return (
      <div className="p-8 text-center">
        <div className="text-red-600 font-bold mb-2">Error loading graph data</div>
        <div className="text-gray-600">{state.error}</div>
      </div>
    );
  }

  // Determine what to show in the path bar: expand dialog takes priority, then hover
  const pathBarNode = expandDialog.open ? null : hoveredNode;
  const showExpandInline = expandDialog.open;

  return (
    <div style={{ display: "flex", gap: "8px", alignItems: "stretch" }}>
      {/* Side panel: filters */}
      {!state.loading && state.allDatasources.length > 0 && (
        <div style={{ width: "240px", flexShrink: 0, height: "600px" }}>
          <GraphViewControls
            datasources={state.allDatasources}
            dsEnabled={state.allDatasources.filter(
              (ds) => !state.dsExclude.has(ds)
            )}
            setDsEnabled={state.toggleDsExclude}
            onMouseoverDs={(ds) => debouncedSetHighlightedDs(ds)}
            onMouseoutDs={() => debouncedSetHighlightedDs(null)}
            edgeTypes={allEdgeTypes}
            hiddenEdgeTypes={state.hiddenEdgeTypes}
            onToggleEdgeType={state.toggleEdgeTypeHidden}
            onShowAllEdgeTypes={state.showAllEdgeTypes}
            onHideAllEdgeTypes={state.hideAllEdgeTypes}
            onMouseoverEdgeType={(et) => debouncedSetHighlightedEt(et)}
            onMouseoutEdgeType={() => debouncedSetHighlightedEt(null)}
          />
        </div>
      )}

      {/* Graph area */}
      <div
        style={{
          position: "relative",
          flex: "1 1 0",
          minWidth: 0,
          height: "600px",
          display: "flex",
          flexDirection: "column",
        }}
      >
        {/* Top path bar */}
        {(() => {
          // Expand dialog heading
          if (showExpandInline) {
            const parentLabel = resolveParentLabel(expandDialog.parentNodeId);
            return (
              <div
                style={{
                  background: "#fafafa",
                  borderBottom: "1px solid #e0e0e0",
                  borderRadius: "8px 8px 0 0",
                  height: "36px",
                  padding: "0 14px",
                  display: "flex",
                  alignItems: "center",
                  gap: "6px",
                  fontSize: "14px",
                  color: "#333",
                  flexShrink: 0,
                }}
              >
                {renderPathBar(
                  null,
                  expandDialog.direction,
                  expandDialog.edgeType,
                  parentLabel,
                  resolveEdgeCount(expandDialog.parentNodeId, expandDialog.direction, expandDialog.edgeType),
                )}
                <div style={{ flex: 1 }} />
                <IconButton onClick={handleCloseDialog} size="small" sx={{ color: "#999" }}>
                  <CloseIcon fontSize="small" />
                </IconButton>
              </div>
            );
          }
          // Hover path bar
          if (pathBarNode && pathBarNode.edgeType) {
            const parentLabel = resolveParentLabel(pathBarNode.parentNodeId);
            const isExpanded = pathBarNode.type === "expanded_node" || pathBarNode.type === "auto_expanded_node";
            return (
              <div
                style={{
                  position: "absolute",
                  top: 0,
                  left: 0,
                  right: 0,
                  zIndex: 10,
                  height: "32px",
                  background: "#fafafa",
                  borderBottom: "1px solid #e0e0e0",
                  borderRadius: "8px 8px 0 0",
                  padding: "0 14px",
                  display: "flex",
                  alignItems: "center",
                  gap: "6px",
                  fontSize: "13px",
                  color: "#333",
                  pointerEvents: "none",
                }}
              >
                {renderPathBar(
                  isExpanded ? pathBarNode.label : null,
                  pathBarNode.direction!,
                  pathBarNode.edgeType,
                  parentLabel,
                  pathBarNode.count,
                )}
              </div>
            );
          }
          return null;
        })()}

        {/* Inline expand panel or graph */}
        {showExpandInline ? (
          <div style={{
            flex: 1,
            display: "flex",
            flexDirection: "column",
            background: "#fff",
            border: "1px solid #e0e0e0",
            borderTop: "none",
            borderRadius: "0 0 8px 8px",
            overflow: "hidden",
          }}>
            <div style={{
              padding: "8px 14px 4px",
              fontSize: "13px",
              fontWeight: 600,
              color: "#666",
              flexShrink: 0,
            }}>
              Choose a node to continue exploring
            </div>
            {state.root && (
              <EdgeExpandPanel
                onSelectNode={handleSelectNodeFromDialog}
                onCancel={handleCloseDialog}
                subgraph={subgraph}
                nodeId={expandDialog.parentNodeId || state.root.getNodeId()}
                encodedNodeId={expandDialog.parentEncodedNodeId || state.root.getEncodedNodeId()}
                direction={expandDialog.direction}
                edgeType={expandDialog.edgeType}
                chain={buildChain()}
                rootLabel={rootLabel}
                dsExclude={state.dsExclude}
              />
            )}
          </div>
        ) : (
          <div style={{ flex: 1, position: "relative", minHeight: 0 }}>
            {state.loading && <LoadingOverlay message="Loading graph..." scoped />}

            {!state.loading && stableLayout.nodes.length > 0 && (
              <GraphRenderer
                layout={stableLayout}
                onClickRoot={handleClickRoot}
                onClickCountNode={handleClickCountNode}
                onClickExpandedNode={handleClickExpandedNode}
                onClickAutoExpandedNode={handleClickAutoExpandedNode}
                onDoubleClickExpandedNode={handleDoubleClickExpandedNode}
                highlightedDatasource={highlightedDs}
                highlightedEdgeType={highlightedEdgeType}
                focusNodeIds={null}
                onHoverNode={setHoveredNode}
                onLeaveNode={() => setHoveredNode(null)}
              />
            )}

            {/* Darken graph and show spinner while an expanded node loads */}
            {!state.loading && state.anyExpansionLoading && (
              <div
                style={{
                  position: "absolute",
                  inset: 0,
                  background: "rgba(0, 0, 0, 0.3)",
                  display: "flex",
                  alignItems: "center",
                  justifyContent: "center",
                  borderRadius: "8px",
                  zIndex: 50,
                  pointerEvents: "none",
                }}
              >
                <LoadingOverlay message="Loading..." scoped />
              </div>
            )}

            {!state.loading && stableLayout.nodes.length <= 1 && (
              <div
                className="flex items-center justify-center h-full text-gray-400"
              >
                No edges found for this node
              </div>
            )}
          </div>
        )}
      </div>
    </div>
  );
}