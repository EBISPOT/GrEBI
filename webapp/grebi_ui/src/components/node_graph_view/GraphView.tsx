import React, { useEffect, useState, useMemo, useCallback, useRef } from "react";
import GraphNode from "../../model/GraphNode";
import GraphNodeRef from "../../model/GraphNodeRef";
import useGraphViewState from "./useGraphViewState";
import { computeRadialLayout, LayoutNode } from "./graphLayout";
import GraphRenderer, { GraphRendererHandle } from "./GraphRenderer";
import GraphViewControls from "./GraphViewControls";
import EdgeExpandPanel, { ChainSegment } from "./EdgeExpandPanel";
import LoadingOverlay from "../LoadingOverlay";
import { expandedKey } from "./useGraphViewState";
import { explorationOf, parseExploration, serialiseExploration } from "./exploration";
import { get } from "../../app/api";
import encodeNodeId from "../../encodeNodeId";
import { ArrowForward as ArrowForwardIcon, Close as CloseIcon, Fullscreen as FullscreenIcon, FullscreenExit as FullscreenExitIcon, Link as LinkIcon, Check as CheckIcon, Download as DownloadIcon } from "@mui/icons-material";
import { IconButton } from "@mui/material";

interface ExpandDialogState {
  open: boolean;
  parentNodeId: string;
  parentEncodedNodeId: string;
  direction: "incoming" | "outgoing";
  edgeType: string;
}

export default function GraphView({
  graph,
  node,
  exploration,
  onExplorationChange,
  onNavigateToNode,
}: {
  graph: string;
  node: GraphNode;
  /** The exploration to show, as serialised by an earlier onExplorationChange (kept in the page's URL). */
  exploration?: string | null;
  /** Told the exploration the view now shows; `replace` when only the filters changed. */
  onExplorationChange?: (exploration: string | null, options: { replace: boolean }) => void;
  /** Where to go when a node in the graph is opened; without it the view re-roots in place. */
  onNavigateToNode?: (node: GraphNodeRef, options: { newTab: boolean }) => void;
}) {
  const state = useGraphViewState(graph);
  const [highlightedDs, setHighlightedDs] = useState<string | null>(null);
  const [highlightedEdgeType, setHighlightedEdgeType] = useState<string | null>(null);
  const [isFullscreen, setIsFullscreen] = useState(false);
  const [linkCopied, setLinkCopied] = useState(false);
  const rendererRef = useRef<GraphRendererHandle>(null);
  const [contextMenu, setContextMenu] = useState<{ node: GraphNodeRef; label: string; x: number; y: number } | null>(null);

  // The exploration last taken from or written to the URL, keyed by root, so
  // the two effects below neither restore what the view just wrote nor write
  // what it just restored.
  const appliedRef = useRef<string | null>(null);
  const chainRef = useRef<string>("");
  const restoringRef = useRef(false);
  const rootId = state.root?.getNodeId();

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

  // Restore the exploration the URL asks for: the filters, then the chain of
  // expansions step by step, fetching each node on the way.
  useEffect(() => {
    if (!onExplorationChange || state.loading || !rootId) return;
    const wanted = exploration || null;
    const key = rootId + "|" + (wanted ?? "");
    if (key === appliedRef.current) return;
    appliedRef.current = key;
    const parsed = parseExploration(wanted);
    chainRef.current = JSON.stringify(parsed ? parsed.steps : []);
    if (!parsed) {
      state.collapseDescendants(rootId);
      state.setFilters([], []);
      return;
    }
    restoringRef.current = true;
    (async () => {
      try {
        state.setFilters(parsed.excludedDatasources, parsed.hiddenEdgeTypes);
        state.collapseDescendants(rootId);
        let parent = rootId;
        for (const step of parsed.steps) {
          const props = await get<any>(`api/v1/graphs/${graph}/nodes/${encodeNodeId(step.nodeId)}`);
          await state.expandEdge(parent, step.direction, step.edgeType, new GraphNodeRef(props));
          parent = step.nodeId;
        }
      } catch (e) {
        console.error("Could not restore the exploration", e);
      } finally {
        restoringRef.current = false;
      }
    })();
  }, [onExplorationChange, exploration, state.loading, rootId]);

  // Tell the page what the view shows now, so the URL follows the exploration.
  useEffect(() => {
    if (!onExplorationChange || state.loading || !rootId || restoringRef.current) return;
    const current = explorationOf(rootId, state.expandedNodes, state.dsExclude, state.hiddenEdgeTypes);
    const serialised = serialiseExploration(current);
    const key = rootId + "|" + (serialised ?? "");
    if (key === appliedRef.current) return;
    const chain = JSON.stringify(current.steps);
    const replace = chain === chainRef.current;
    chainRef.current = chain;
    appliedRef.current = key;
    onExplorationChange(serialised, { replace });
  }, [onExplorationChange, state.loading, rootId, state.expandedNodes, state.dsExclude, state.hiddenEdgeTypes]);

  const copyLink = useCallback(() => {
    const done = () => {
      setLinkCopied(true);
      setTimeout(() => setLinkCopied(false), 1500);
    };
    if (typeof navigator !== "undefined" && navigator.clipboard) {
      navigator.clipboard.writeText(window.location.href).then(done).catch(() => {});
    }
  }, []);

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

  /** The node an expanded or auto-expanded slot shows. */
  const nodeAt = useCallback(
    (parentNodeId: string, direction: "incoming" | "outgoing", edgeType: string): GraphNodeRef | undefined => {
      const key = expandedKey(parentNodeId, direction, edgeType);
      return state.expandedNodes.get(key)?.node || state.autoExpandedNodes.get(key);
    },
    [state.expandedNodes, state.autoExpandedNodes]
  );

  const handleDoubleClickExpandedNode = useCallback(
    (parentNodeId: string, direction: "incoming" | "outgoing", edgeType: string, nodeId: string, options?: { newTab: boolean }) => {
      const target = nodeAt(parentNodeId, direction, edgeType);
      if (!target) return;
      // On a node page the node's own page is the place to continue from;
      // elsewhere the view re-roots in place.
      if (onNavigateToNode) {
        onNavigateToNode(target, { newTab: !!options?.newTab });
      } else {
        state.loadEdgeCounts(target);
      }
    },
    [nodeAt, state.loadEdgeCounts, onNavigateToNode]
  );

  const handleContextMenuNode = useCallback(
    (layoutNode: LayoutNode, x: number, y: number) => {
      if (!layoutNode.parentNodeId || !layoutNode.direction || !layoutNode.edgeType) return;
      const target = nodeAt(layoutNode.parentNodeId, layoutNode.direction, layoutNode.edgeType);
      if (!target) return;
      setContextMenu({ node: target, label: layoutNode.label, x, y });
    },
    [nodeAt]
  );

  const downloadImage = useCallback(() => {
    const name = (state.root?.getName() || "graph").replace(/[^\w.-]+/g, "_");
    rendererRef.current?.downloadImage(`${name}-graph`);
  }, [state.root]);

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
    <div style={{
      display: "flex",
      gap: "8px",
      alignItems: "stretch",
      ...(isFullscreen ? {
        position: "fixed" as const,
        inset: 0,
        zIndex: 9999,
        background: "#fff",
        padding: "8px",
      } : {}),
    }}>
      {/* Side panel: filters */}
      {!state.loading && state.allDatasources.length > 0 && (
        <div style={{ width: "240px", flexShrink: 0, height: isFullscreen ? "100%" : "600px" }}>
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
          height: isFullscreen ? "100%" : "600px",
          display: "flex",
          flexDirection: "column",
        }}
      >
        {/* Save the drawing as an image */}
        <IconButton
          onClick={downloadImage}
          size="small"
          title="Download as image"
          sx={{
            position: "absolute",
            top: 4,
            right: onExplorationChange ? 76 : 40,
            zIndex: 20,
            background: "rgba(255,255,255,0.85)",
            "&:hover": { background: "rgba(255,255,255,1)" },
          }}
        >
          <DownloadIcon fontSize="small" />
        </IconButton>
        {/* Copy a link to this exploration, when the URL carries it */}
        {onExplorationChange && (
          <IconButton
            onClick={copyLink}
            size="small"
            title={linkCopied ? "Link copied" : "Copy link to this view"}
            sx={{
              position: "absolute",
              top: 4,
              right: 40,
              zIndex: 20,
              background: "rgba(255,255,255,0.85)",
              "&:hover": { background: "rgba(255,255,255,1)" },
            }}
          >
            {linkCopied ? <CheckIcon fontSize="small" /> : <LinkIcon fontSize="small" />}
          </IconButton>
        )}
        {/* Fullscreen toggle */}
        <IconButton
          onClick={() => setIsFullscreen((f) => !f)}
          size="small"
          title={isFullscreen ? "Exit fullscreen" : "Fullscreen"}
          sx={{
            position: "absolute",
            top: 4,
            right: 4,
            zIndex: 20,
            background: "rgba(255,255,255,0.85)",
            "&:hover": { background: "rgba(255,255,255,1)" },
          }}
        >
          {isFullscreen ? <FullscreenExitIcon fontSize="small" /> : <FullscreenIcon fontSize="small" />}
        </IconButton>
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
                graph={graph}
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
                ref={rendererRef}
                layout={stableLayout}
                onClickRoot={handleClickRoot}
                onClickCountNode={handleClickCountNode}
                onClickExpandedNode={handleClickExpandedNode}
                onClickAutoExpandedNode={handleClickAutoExpandedNode}
                onDoubleClickExpandedNode={handleDoubleClickExpandedNode}
                onContextMenuNode={handleContextMenuNode}
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

            {/* The menu of a right-clicked node */}
            {contextMenu && (
              <>
                <div style={{ position: "absolute", inset: 0, zIndex: 60 }} onClick={() => setContextMenu(null)} onContextMenu={(e) => { e.preventDefault(); setContextMenu(null); }} />
                <ul
                  role="menu"
                  aria-label={contextMenu.label}
                  className="bg-white border border-gray-200 rounded-md shadow-lg py-1 text-sm"
                  style={{ position: "absolute", left: contextMenu.x, top: contextMenu.y, zIndex: 61, minWidth: 180 }}
                >
                  <li className="px-3 py-1 text-gray-500 truncate border-b border-gray-100" style={{ maxWidth: 260 }}>{contextMenu.label}</li>
                  {onNavigateToNode ? (
                    <>
                      <li role="menuitem" className="px-3 py-1 hover:bg-gray-100 cursor-pointer" onClick={() => { onNavigateToNode(contextMenu.node, { newTab: false }); setContextMenu(null); }}>Open node page</li>
                      <li role="menuitem" className="px-3 py-1 hover:bg-gray-100 cursor-pointer" onClick={() => { onNavigateToNode(contextMenu.node, { newTab: true }); setContextMenu(null); }}>Open in a new tab</li>
                    </>
                  ) : (
                    <li role="menuitem" className="px-3 py-1 hover:bg-gray-100 cursor-pointer" onClick={() => { state.loadEdgeCounts(contextMenu.node); setContextMenu(null); }}>Explore from here</li>
                  )}
                  <li role="menuitem" className="px-3 py-1 hover:bg-gray-100 cursor-pointer" onClick={() => { navigator.clipboard?.writeText(contextMenu.node.getNodeId()).catch(() => {}); setContextMenu(null); }}>Copy node id</li>
                </ul>
              </>
            )}
          </div>
        )}
      </div>
    </div>
  );
}