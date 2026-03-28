import React, { useEffect, useRef } from "react";
import Graph from "graphology";
import Sigma from "sigma";
import { createEdgeArrowProgram, NodeProgram } from "sigma/rendering";
import { createEdgeCurveProgram, indexParallelEdgesIndex, createDrawCurvedEdgeLabel } from "@sigma/edge-curve";
import { DEFAULT_EDGE_ARROW_HEAD_PROGRAM_OPTIONS } from "sigma/rendering";
import type { EdgeLabelDrawingFunction } from "sigma/rendering";

const BigArrowProgram = createEdgeArrowProgram({
  lengthToThicknessRatio: 5,
  widenessToThicknessRatio: 4,
});

const baseCurvedEdgeLabelDrawer = createDrawCurvedEdgeLabel({
  curvatureAttribute: "curvature",
  defaultCurvature: 0.15,
  arrowHead: null,
});

/**
 * Edge label drawer that hides labels on faded edges (from DS/ET highlighting).
 */
const edgeLabelDrawer: EdgeLabelDrawingFunction = (
  context, edgeData, sourceData, targetData, settings,
) => {
  if ((edgeData as any)._faded) {
    return;
  }
  baseCurvedEdgeLabelDrawer(context, edgeData, sourceData, targetData, settings);
};

/**
 * Custom curved arrow program that uses our hover-aware edge label drawer.
 * EdgeCurvedArrowProgram has its own drawLabel that overrides defaultDrawEdgeLabel,
 * so we must pass our custom drawer directly to createEdgeCurveProgram.
 */
const CurvedArrowProgram = createEdgeCurveProgram({
  arrowHead: DEFAULT_EDGE_ARROW_HEAD_PROGRAM_OPTIONS,
  drawLabel: edgeLabelDrawer,
});

/**
 * Sets nodeReducer and edgeReducer for datasource/edge-type highlighting.
 * No hover effects — those are handled via the bottom path bar.
 */
function updateReducers(
  sigma: Sigma,
  highlightedDsRef: React.MutableRefObject<string | null>,
  highlightedEtRef: React.MutableRefObject<string | null>,
) {
  const FADED_EDGE_COLOR = "#e0e0e0";

  const hiDs = highlightedDsRef.current;
  const hiEt = highlightedEtRef.current;

  sigma.setSetting("nodeReducer", (nodeKey: string, data: any) => {
    if (!hiDs && !hiEt) return data;

    const res = { ...data };

    if (hiDs && data.datasources) {
      const dss = data.datasources as string[];
      if (dss.includes(hiDs)) {
        res.color = hiDs.startsWith("OLS.") ? "#00827c" : "#7323b7";
        res.labelColor = "#ffffff";
        if (data.dsToCount && data.dsToCount[hiDs]) {
          res.label = formatter.format(data.dsToCount[hiDs]);
        }
      } else if (data.nodeType !== "root") {
        res.color = "rgba(0,0,0,0)";
        res.label = "";
        res.labelColor = "rgba(0,0,0,0)";
      }
    }

    if (hiEt) {
      if (data.edgeType === hiEt) {
        res.color = "#2196f3";
        res.labelColor = data.nodeType === "count" ? "#333333" : "#ffffff";
      } else if (data.nodeType !== "root") {
        res.color = "rgba(0,0,0,0)";
        res.label = "";
        res.labelColor = "rgba(0,0,0,0)";
      }
    }

    return res;
  });

  sigma.setSetting("edgeReducer", (edgeKey: string, data: any) => {
    if (!hiDs && !hiEt) return data;

    const res = { ...data };

    if (hiDs && data.datasources) {
      const dss = data.datasources as string[];
      if (dss.includes(hiDs)) {
        res.color = hiDs.startsWith("OLS.") ? "#00827c" : "#7323b7";
        res.size = Math.max(data.size, 2);
        res._faded = false;
      } else {
        res.color = FADED_EDGE_COLOR;
        res.size = 0.5;
        res._faded = true;
      }
    }

    if (hiEt) {
      if (data.edgeType === hiEt) {
        res.color = "#2196f3";
        res.size = Math.max(data.size, 2);
        res._faded = false;
      } else {
        res.color = FADED_EDGE_COLOR;
        res.size = 0.5;
        res._faded = true;
      }
    }

    return res;
  });

  sigma.refresh();
}

/**
 * No-op WebGL hover program.  Sigma v3 renders a WebGL node disc on the
 * "hoverNodes" layer which sits *above* the 2D-canvas "hovers" layer.
 * Because our custom drawNodeHover paints the label *inside* the circle
 * on the canvas layer, the WebGL disc would cover it.  This empty program
 * prevents that.
 */
class NoOpNodeHoverProgram extends NodeProgram {
  getDefinition() {
    return {
      VERTICES: 0,
      VERTEX_SHADER_SOURCE: "void main(){gl_Position=vec4(0);}",
      FRAGMENT_SHADER_SOURCE: "void main(){discard;}",
      METHOD: WebGLRenderingContext.POINTS,
      UNIFORMS: [] as string[],
      ATTRIBUTES: [],
    };
  }
  processVisibleItem() {}
  setUniforms() {}
  draw() {}
}
import { GraphLayout, LayoutNode } from "./graphLayout";

const formatter = Intl.NumberFormat("en", { notation: "compact" });

export interface GraphRendererProps {
  layout: GraphLayout;
  onClickRoot: () => void;
  onClickCountNode: (parentNodeId: string, parentEncodedNodeId: string, direction: "incoming" | "outgoing", edgeType: string) => void;
  onClickExpandedNode: (parentNodeId: string, parentEncodedNodeId: string, direction: "incoming" | "outgoing", edgeType: string) => void;
  onClickAutoExpandedNode: (parentNodeId: string, parentEncodedNodeId: string, direction: "incoming" | "outgoing", edgeType: string) => void;
  onDoubleClickExpandedNode: (parentNodeId: string, direction: "incoming" | "outgoing", edgeType: string, nodeId: string) => void;
  highlightedDatasource: string | null;
  highlightedEdgeType: string | null;
  /** Node IDs to zoom into after layout update (parent + newly expanded nodes) */
  focusNodeIds: string[] | null;
  onHoverNode?: (node: LayoutNode) => void;
  onLeaveNode?: () => void;
}

/**
 * Sigma.js-based graph renderer.
 *
 * This component maintains a single Sigma instance across renders.
 * The graphology Graph is rebuilt each time `layout` changes, and
 * sigma's nodeReducer/edgeReducer are updated via setSetting when
 * the `highlightedDatasource` prop changes.
 */
export default function GraphRenderer({
  layout,
  onClickRoot,
  onClickCountNode,
  onClickExpandedNode,
  onClickAutoExpandedNode,
  onDoubleClickExpandedNode,
  highlightedDatasource,
  highlightedEdgeType,
  focusNodeIds,
  onHoverNode,
  onLeaveNode,
}: GraphRendererProps) {
  const containerRef = useRef<HTMLDivElement>(null);
  const sigmaRef = useRef<Sigma | null>(null);
  const graphRef = useRef<Graph | null>(null);

  // Use refs for callbacks so event handlers always call the latest version
  const onClickRootRef = useRef(onClickRoot);
  onClickRootRef.current = onClickRoot;
  const onClickCountNodeRef = useRef(onClickCountNode);
  onClickCountNodeRef.current = onClickCountNode;
  const onClickExpandedNodeRef = useRef(onClickExpandedNode);
  onClickExpandedNodeRef.current = onClickExpandedNode;
  const onClickAutoExpandedNodeRef = useRef(onClickAutoExpandedNode);
  onClickAutoExpandedNodeRef.current = onClickAutoExpandedNode;
  const onDoubleClickExpandedNodeRef = useRef(onDoubleClickExpandedNode);
  onDoubleClickExpandedNodeRef.current = onDoubleClickExpandedNode;

  // Use refs for hover callbacks
  const onHoverNodeRef = useRef(onHoverNode);
  onHoverNodeRef.current = onHoverNode;
  const onLeaveNodeRef = useRef(onLeaveNode);
  onLeaveNodeRef.current = onLeaveNode;

  // Store layout nodes by ID for lookup
  const layoutNodesRef = useRef<Map<string, LayoutNode>>(new Map());

  // Build graphology Graph from layout data
  function buildGraph(layoutData: GraphLayout): Graph {
    const graph = new Graph({ multi: true });

    for (const node of layoutData.nodes) {
      graph.addNode(node.id, {
        x: node.x,
        y: node.y,
        size: node.size,
        label: node.label,
        color: node.color,
        nodeType: node.type,
        direction: node.direction,
        edgeType: node.edgeType,
        count: node.count,
        datasources: node.datasources,
        dsToCount: node.dsToCount,
        labelColor: node.labelColor,
        parentNodeId: node.parentNodeId,
        parentEncodedNodeId: node.parentEncodedNodeId,
        hasHiddenChildren: node.hasHiddenChildren || false,
        forceLabel: true,
      });
    }

    for (const edge of layoutData.edges) {
      if (!graph.hasNode(edge.source) || !graph.hasNode(edge.target)) continue;
      graph.addEdge(edge.source, edge.target, {
        label: edge.label,
        color: edge.color,
        size: edge.size,
        type: "curvedArrow",
        edgeType: edge.edgeType,
        direction: edge.direction,
        datasources: edge.datasources,
        forceLabel: true,
      });
    }

    // Index parallel edges so curved arrows get appropriate curvature offsets
    indexParallelEdgesIndex(graph);

    // Set curvature: non-parallel edges get a gentle curve, parallel/bidirectional
    // edges get stronger curvature so they separate visually.
    graph.forEachEdge((edge) => {
      const parallelIndex = graph.getEdgeAttribute(edge, "parallelIndex");
      const parallelMaxIndex = graph.getEdgeAttribute(edge, "parallelMaxIndex");

      if (parallelIndex != null && parallelMaxIndex != null) {
        const curvature = 0.5 * (parallelIndex / parallelMaxIndex);
        graph.setEdgeAttribute(edge, "curvature", curvature);
      } else {
        // Single edge — small curvature so it's still visible as a curve
        graph.setEdgeAttribute(edge, "curvature", 0.15);
      }
    });

    return graph;
  }

  // Initialize sigma once, update graph on subsequent renders
  useEffect(() => {
    if (!containerRef.current) return;

    // Update layout node lookup
    const nodeMap = new Map<string, LayoutNode>();
    for (const n of layout.nodes) {
      nodeMap.set(n.id, n);
    }
    layoutNodesRef.current = nodeMap;

    const graph = buildGraph(layout);

    if (sigmaRef.current) {
      // Update existing sigma instance with new graph
      graphRef.current = graph;
      sigmaRef.current.setGraph(graph);
      sigmaRef.current.refresh();
      return;
    }

    // First render: create sigma
    graphRef.current = graph;

    const sigma = new Sigma(graph, containerRef.current, {
      allowInvalidContainer: true,
      renderLabels: true,
      renderEdgeLabels: true,
      enableEdgeEvents: false,
      defaultEdgeType: "curvedArrow",
      edgeProgramClasses: {
        arrow: BigArrowProgram,
        curvedArrow: CurvedArrowProgram,
      },
      labelFont: "'Inter', 'Helvetica Neue', Arial, sans-serif",
      labelSize: 14,
      labelWeight: "500",
      labelColor: { attribute: "labelColor", color: "#333" },
      edgeLabelFont: "'Inter', 'Helvetica Neue', Arial, sans-serif",
      edgeLabelSize: 11,
      edgeLabelColor: { color: "#888" },
      defaultDrawEdgeLabel: edgeLabelDrawer,
      labelDensity: 2,
      labelRenderedSizeThreshold: 1,
      stagePadding: 60,
      minEdgeThickness: 1,
      zoomDuration: 200,
      minCameraRatio: 1,
      autoRescale: true,
      autoCenter: true,
      itemSizesReference: "positions",
      defaultDrawNodeLabel: drawNodeLabel,
      defaultDrawNodeHover: drawNodeHover,
      nodeHoverProgramClasses: {
        circle: NoOpNodeHoverProgram,
      },
    });

    sigmaRef.current = sigma;

    // Set node + edge reducers (handles DS/ET highlight)
    updateReducers(sigma, highlightedDsRef, highlightedEtRef);

    // Click handler
    sigma.on("clickNode", ({ node }) => {
      const attrs = sigma.getGraph().getNodeAttributes(node);
      if (attrs.nodeType === "root") {
        onClickRootRef.current();
      } else if (attrs.nodeType === "count") {
        onClickCountNodeRef.current(attrs.parentNodeId, attrs.parentEncodedNodeId, attrs.direction, attrs.edgeType);
      } else if (attrs.nodeType === "auto_expanded_node") {
        onClickAutoExpandedNodeRef.current(attrs.parentNodeId, attrs.parentEncodedNodeId, attrs.direction, attrs.edgeType);
      } else if (attrs.nodeType === "expanded_node") {
        onClickExpandedNodeRef.current(attrs.parentNodeId, attrs.parentEncodedNodeId, attrs.direction, attrs.edgeType);
      }
    });

    // Double-click handler
    sigma.on("doubleClickNode", ({ node, event }) => {
      event.preventSigmaDefault();
      const attrs = sigma.getGraph().getNodeAttributes(node);
      if (attrs.nodeType === "expanded_node") {
        onDoubleClickExpandedNodeRef.current(attrs.parentNodeId, attrs.direction, attrs.edgeType, node);
      } else if (attrs.nodeType === "auto_expanded_node") {
        onDoubleClickExpandedNodeRef.current(attrs.parentNodeId, attrs.direction, attrs.edgeType, node);
      }
    });

    // Hover handlers — just cursor + path bar, no graph effects
    sigma.on("enterNode", ({ node }) => {
      const attrs = sigma.getGraph().getNodeAttributes(node);

      if (containerRef.current) {
        if (
          attrs.nodeType === "root" ||
          attrs.nodeType === "count" ||
          attrs.nodeType === "expanded_node" ||
          attrs.nodeType === "auto_expanded_node"
        ) {
          containerRef.current.style.cursor = "pointer";
        }
      }

      const layoutNode = layoutNodesRef.current.get(node);
      if (layoutNode && attrs.nodeType !== "root") {
        onHoverNodeRef.current?.(layoutNode);
      }
    });

    sigma.on("leaveNode", () => {
      onLeaveNodeRef.current?.();
      if (containerRef.current) {
        containerRef.current.style.cursor = "default";
      }
    });
  }, [layout]);

  // Zoom to fit entire graph when layout changes
  useEffect(() => {
    if (!sigmaRef.current || !graphRef.current) return;
    const sigma = sigmaRef.current;
    // Reset camera to show all nodes
    sigma.getCamera().animate({ x: 0.5, y: 0.5, ratio: 1 }, { duration: 300 });
  }, [layout]);

  // Update reducers when highlight changes — use refs + direct sigma
  // calls to avoid React render overhead which causes sluggish hovering.
  const highlightedDsRef = useRef(highlightedDatasource);
  const highlightedEtRef = useRef(highlightedEdgeType);

  useEffect(() => {
    if (!sigmaRef.current) return;
    const dsChanged = highlightedDsRef.current !== highlightedDatasource;
    const etChanged = highlightedEtRef.current !== highlightedEdgeType;
    if (!dsChanged && !etChanged) return;

    highlightedDsRef.current = highlightedDatasource;
    highlightedEtRef.current = highlightedEdgeType;

    updateReducers(sigmaRef.current, highlightedDsRef, highlightedEtRef);
  }, [highlightedDatasource, highlightedEdgeType]);

  // Cleanup on unmount
  useEffect(() => {
    return () => {
      if (sigmaRef.current) {
        sigmaRef.current.kill();
        sigmaRef.current = null;
        graphRef.current = null;
      }
    };
  }, []);

  return (
    <div style={{ position: "relative", width: "100%", height: "100%" }}>
      <div
        ref={containerRef}
        style={{
          width: "100%",
          height: "100%",
          background: "#fafafa",
          borderRadius: "8px",
          border: "1px solid #e0e0e0",
        }}
      />
    </div>
  );
}

/**
 * Custom label renderer: draw count labels inside the circle,
 * draw other labels beside the node.
 */
function drawNodeLabel(
  context: CanvasRenderingContext2D,
  data: any,
  settings: any,
): void {
  const nodeType = data.nodeType;

  const label = data.label;
  if (!label) return;

  // All node types: draw label centred inside the circle
  if (nodeType === "count") {
    // Italic numeric labels
    const fontSize = Math.max(data.size * 0.45, 9);
    context.fillStyle = data.labelColor || "#666666";
    context.font = `italic ${fontSize}px 'Inter', 'Helvetica Neue', Arial, sans-serif`;
    context.textAlign = "center";
    context.textBaseline = "middle";
    context.fillText(label, data.x, data.y);
  } else {
    // Text labels (root, expanded_node, etc.) – truncate to fit circle
    const maxWidth = data.size * 1.6;
    const fontSize = Math.max(data.size * 0.35, 9);
    context.font = `bold ${fontSize}px 'Inter', 'Helvetica Neue', Arial, sans-serif`;

    let displayLabel = label;
    if (context.measureText(displayLabel).width > maxWidth) {
      while (displayLabel.length > 1 && context.measureText(displayLabel + "\u2026").width > maxWidth) {
        displayLabel = displayLabel.slice(0, -1);
      }
      displayLabel = displayLabel + "\u2026";
    }

    context.fillStyle = data.labelColor || "#ffffff";
    context.textAlign = "center";
    context.textBaseline = "middle";
    context.fillText(displayLabel, data.x, data.y);

    // Draw ring indicator if node has hidden children
    if (data.hasHiddenChildren) {
      context.beginPath();
      context.arc(data.x, data.y, data.size + 3, 0, Math.PI * 2);
      context.strokeStyle = "#ff9800";
      context.lineWidth = 2.5;
      context.setLineDash([4, 3]);
      context.stroke();
      context.setLineDash([]);
    }
  }
}

/**
 * Custom hover renderer: redraw the node circle and label so they don't disappear.
 * Tooltips are rendered as React overlays.
 */
function drawNodeHover(
  context: CanvasRenderingContext2D,
  data: any,
  settings: any,
): void {
  // Redraw the node circle
  context.beginPath();
  context.arc(data.x, data.y, data.size, 0, Math.PI * 2);
  context.fillStyle = data.color || "#999";
  context.fill();
  context.closePath();

  // Then redraw the label on top
  drawNodeLabel(context, data, settings);
}
