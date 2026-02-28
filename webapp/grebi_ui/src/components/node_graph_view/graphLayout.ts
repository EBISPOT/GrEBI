import { AggregatedEdgeCount, ExpandedNodeState, aggregateCounts, expandedKey } from "./useGraphViewState";
import GraphNodeRef from "../../model/GraphNodeRef";

const MIN_COUNT_NODE_SIZE = 20;
const MAX_COUNT_NODE_SIZE = 60;
const INNER_RADIUS = 250;
const COLLISION_PADDING = 15; // extra padding between nodes during collision resolution
const COLLISION_ITERATIONS = 30; // number of collision resolution passes

export interface LayoutNode {
  id: string;
  x: number;
  y: number;
  size: number;
  label: string;
  type: "root" | "count" | "expanded_node" | "auto_expanded_node";
  hasHiddenChildren?: boolean;
  color: string;
  labelColor: string;
  // Metadata for interactions
  direction?: "incoming" | "outgoing";
  edgeType?: string;
  count?: number;
  datasources?: string[];
  dsToCount?: { [ds: string]: number };
  /** The data (real) node ID of the parent whose edges these counts belong to */
  parentNodeId?: string;
  /** Encoded version for API calls */
  parentEncodedNodeId?: string;
}

export interface LayoutEdge {
  id: string;
  source: string;
  target: string;
  label: string;
  color: string;
  type: "stem" | "expanded";
  dashed: boolean;
  size: number;
  direction?: "incoming" | "outgoing";
  edgeType?: string;
  datasources?: string[];
}

export interface GraphLayout {
  nodes: LayoutNode[];
  edges: LayoutEdge[];
}

const formatter = Intl.NumberFormat("en", { notation: "compact" });

function countToSize(count: number, maxCount: number): number {
  if (maxCount <= 0) return MIN_COUNT_NODE_SIZE;
  const ratio = Math.min(count / maxCount, 1);
  return MIN_COUNT_NODE_SIZE + ratio * (MAX_COUNT_NODE_SIZE - MIN_COUNT_NODE_SIZE);
}

export function computeRadialLayout(
  rootId: string,
  rootLabel: string,
  rootEncodedId: string,
  incomingAggregated: AggregatedEdgeCount[],
  outgoingAggregated: AggregatedEdgeCount[],
  expandedNodes: Map<string, ExpandedNodeState>,
  dsExclude: Set<string>,
  hiddenEdgeTypes: Set<string>,
  autoExpandedNodes: Map<string, GraphNodeRef>,
): GraphLayout {
  const nodes: LayoutNode[] = [];
  const edges: LayoutEdge[] = [];
  const usedNodeIds = new Set<string>();

  // Check if root has an expansion chain below it
  const rootHasExpansion = Array.from(expandedNodes.keys()).some(k => k.startsWith(rootId + "::"));

  // Root node at center
  nodes.push({
    id: rootId,
    x: 0,
    y: 0,
    size: 40,
    label: rootLabel,
    type: "root",
    color: "#555555",
    labelColor: "#ffffff",
    hasHiddenChildren: rootHasExpansion,
  });
  usedNodeIds.add(rootId);

  // Compute global max count for sizing
  let maxCount = 0;
  for (const agg of [...incomingAggregated, ...outgoingAggregated]) {
    maxCount = Math.max(maxCount, agg.totalCount);
  }

  const totalStems = incomingAggregated.length + outgoingAggregated.length;
  if (totalStems === 0) return { nodes, edges };

  // Gap (in radians) at the top and bottom to visually separate incoming/outgoing
  const GAP = 0.5;

  // Incoming: left semicircle (PI/2 → 3PI/2) with gap inset
  layoutChildren(
    rootId, rootEncodedId,
    0, 0,
    incomingAggregated, "incoming",
    Math.PI / 2 + GAP, (3 * Math.PI) / 2 - GAP,
    INNER_RADIUS,
    expandedNodes, dsExclude, hiddenEdgeTypes,
    nodes, edges, usedNodeIds, maxCount, 0, autoExpandedNodes,
  );

  // Outgoing: right semicircle (-PI/2 → PI/2) with gap inset
  layoutChildren(
    rootId, rootEncodedId,
    0, 0,
    outgoingAggregated, "outgoing",
    -Math.PI / 2 + GAP, Math.PI / 2 - GAP,
    INNER_RADIUS,
    expandedNodes, dsExclude, hiddenEdgeTypes,
    nodes, edges, usedNodeIds, maxCount, 0, autoExpandedNodes,
  );

  // Resolve overlapping nodes by pushing them apart
  resolveCollisions(nodes);

  return { nodes, edges };
}

/**
 * Lay out children for one parent node.
 *
 * If the parent has an expansion, only the expanded edge is shown (no other
 * children visible). If the parent is a "leaf" (no expansion), all children
 * are shown with de-duplicated singulars (auto-expanded nodes sharing the
 * same target ID collapse into one graph node with multiple edges).
 *
 * Since no node can appear twice, every real node uses its actual node ID
 * as the graph ID. Only synthetic count bubbles use a namespaced ID.
 */
function layoutChildren(
  parentId: string,
  parentEncodedId: string,
  parentX: number,
  parentY: number,
  aggregated: AggregatedEdgeCount[],
  direction: "incoming" | "outgoing",
  startAngle: number,
  endAngle: number,
  radius: number,
  expandedNodes: Map<string, ExpandedNodeState>,
  dsExclude: Set<string>,
  hiddenEdgeTypes: Set<string>,
  nodes: LayoutNode[],
  edges: LayoutEdge[],
  usedNodeIds: Set<string>,
  maxCount: number,
  depth: number,
  autoExpandedNodes: Map<string, GraphNodeRef>,
) {
  if (aggregated.length === 0) return;

  // Does this parent have any expansion (in any direction)?
  const hasExpansion = Array.from(expandedNodes.keys()).some(k => k.startsWith(parentId + "::"));

  if (hasExpansion) {
    // --- Parent has an expansion: only show the expanded edge for this direction ---
    const expandedAgg = aggregated.filter(agg => {
      const key = expandedKey(parentId, direction, agg.edgeType);
      return expandedNodes.has(key);
    });
    if (expandedAgg.length === 0) return; // expansion is in other direction

    // At most one expansion per parent (enforced by expandEdge)
    const agg = expandedAgg[0];
    const expanded = expandedNodes.get(expandedKey(parentId, direction, agg.edgeType))!;

    const angle = (startAngle + endAngle) / 2;
    const ecx = parentX + Math.cos(angle) * radius;
    const ecy = parentY + Math.sin(angle) * radius;

    // Use actual node ID — skip self-references / back-references
    const expNodeId = expanded.node.getNodeId();
    if (usedNodeIds.has(expNodeId)) return;

    const expHasExpansion = Array.from(expandedNodes.keys()).some(k => k.startsWith(expNodeId + "::"));

    usedNodeIds.add(expNodeId);
    nodes.push({
      id: expNodeId,
      x: ecx,
      y: ecy,
      size: 28,
      label: expanded.node.getName(),
      type: "expanded_node",
      color: "#4a90d9",
      labelColor: "#ffffff",
      direction,
      edgeType: agg.edgeType,
      parentNodeId: parentId,
      parentEncodedNodeId: parentEncodedId,
      hasHiddenChildren: expHasExpansion,
    });

    // Direct edge from parent to expanded node
    const edgeId = `edge::${parentId}::${direction}::${agg.edgeType}`;
    if (direction === "outgoing") {
      edges.push({ id: edgeId, source: parentId, target: expNodeId, label: agg.edgeType, color: "#4a90d9", type: "expanded", dashed: false, size: 2, direction, edgeType: agg.edgeType, datasources: agg.datasources });
    } else {
      edges.push({ id: edgeId, source: expNodeId, target: parentId, label: agg.edgeType, color: "#4a90d9", type: "expanded", dashed: false, size: 2, direction, edgeType: agg.edgeType, datasources: agg.datasources });
    }

    // Recurse into expanded node's children (if loaded)
    if (!expanded.loading) {
      const subIncoming = aggregateCounts(expanded.incomingEdgeCounts, dsExclude, hiddenEdgeTypes);
      const subOutgoing = aggregateCounts(expanded.outgoingEdgeCounts, dsExclude, hiddenEdgeTypes);

      const coneSpan = Math.PI * 1.2;
      const subMid = angle;
      const nextRadius = Math.max(radius * 0.65, 100);

      if (subIncoming.length > 0) {
        layoutChildren(
          expNodeId, expanded.node.getEncodedNodeId(),
          ecx, ecy,
          subIncoming, "incoming",
          subMid - coneSpan / 2, subMid,
          nextRadius,
          expandedNodes, dsExclude, hiddenEdgeTypes,
          nodes, edges, usedNodeIds, maxCount, depth + 1, autoExpandedNodes,
        );
      }

      if (subOutgoing.length > 0) {
        layoutChildren(
          expNodeId, expanded.node.getEncodedNodeId(),
          ecx, ecy,
          subOutgoing, "outgoing",
          subMid, subMid + coneSpan / 2,
          nextRadius,
          expandedNodes, dsExclude, hiddenEdgeTypes,
          nodes, edges, usedNodeIds, maxCount, depth + 1, autoExpandedNodes,
        );
      }
    }
  } else {
    // --- No expansion: show all children (with de-duplicated singulars) ---

    // Separate singulars (auto-expanded count=1) from regular counts
    const singularAggs: AggregatedEdgeCount[] = [];
    const countAggs: AggregatedEdgeCount[] = [];

    for (const agg of aggregated) {
      const autoKey = expandedKey(parentId, direction, agg.edgeType);
      if (agg.totalCount === 1 && autoExpandedNodes.has(autoKey)) {
        singularAggs.push(agg);
      } else {
        countAggs.push(agg);
      }
    }

    // De-duplicate singulars by target node ID: if multiple edge types
    // each have count=1 pointing to the same node, show one node with
    // multiple labelled edges.
    const singularsByNodeId = new Map<string, { node: GraphNodeRef; aggs: AggregatedEdgeCount[] }>();
    for (const agg of singularAggs) {
      const autoKey = expandedKey(parentId, direction, agg.edgeType);
      const autoNode = autoExpandedNodes.get(autoKey)!;
      const nodeId = autoNode.getNodeId();
      if (!singularsByNodeId.has(nodeId)) {
        singularsByNodeId.set(nodeId, { node: autoNode, aggs: [] });
      }
      singularsByNodeId.get(nodeId)!.aggs.push(agg);
    }

    const totalItems = countAggs.length + singularsByNodeId.size;
    if (totalItems === 0) return;

    const angleStep = (endAngle - startAngle) / (totalItems + 1);
    let slotIndex = 0;

    // Layout de-duplicated singulars — use real node ID as graph ID
    // Skip self-references (edges pointing back to an already-visible node)
    for (const [nodeId, { node: autoNode, aggs }] of singularsByNodeId) {
      if (usedNodeIds.has(nodeId)) continue;

      slotIndex++;
      const angle = startAngle + angleStep * slotIndex;
      const cx = parentX + Math.cos(angle) * radius;
      const cy = parentY + Math.sin(angle) * radius;

      usedNodeIds.add(nodeId);
      nodes.push({
        id: nodeId,
        x: cx,
        y: cy,
        size: 28,
        label: autoNode.getName(),
        type: "auto_expanded_node",
        color: "#4a90d9",
        labelColor: "#ffffff",
        direction,
        edgeType: aggs[0].edgeType,
        count: 1,
        datasources: aggs.flatMap(a => a.datasources).filter((v, i, arr) => arr.indexOf(v) === i),
        dsToCount: aggs.reduce<{ [ds: string]: number }>((acc, a) => ({ ...acc, ...a.dsToCount }), {}),
        parentNodeId: parentId,
        parentEncodedNodeId: parentEncodedId,
      });

      // One edge per edge type pointing to the same node
      for (const agg of aggs) {
        const edgeId = `edge::${parentId}::${direction}::${agg.edgeType}`;
        if (direction === "outgoing") {
          edges.push({ id: edgeId, source: parentId, target: nodeId, label: agg.edgeType, color: "#4a90d9", type: "expanded", dashed: false, size: 2, direction, edgeType: agg.edgeType, datasources: agg.datasources });
        } else {
          edges.push({ id: edgeId, source: nodeId, target: parentId, label: agg.edgeType, color: "#4a90d9", type: "expanded", dashed: false, size: 2, direction, edgeType: agg.edgeType, datasources: agg.datasources });
        }
      }
    }

    // Layout regular count bubbles
    for (const agg of countAggs) {
      slotIndex++;
      const angle = startAngle + angleStep * slotIndex;
      const cx = parentX + Math.cos(angle) * radius;
      const cy = parentY + Math.sin(angle) * radius;
      const countNodeId = `count::${parentId}::${direction}::${agg.edgeType}`;

      nodes.push({
        id: countNodeId,
        x: cx,
        y: cy,
        size: countToSize(agg.totalCount, maxCount),
        label: formatter.format(agg.totalCount),
        type: "count",
        color: "#e8e8e8",
        labelColor: "#666666",
        direction,
        edgeType: agg.edgeType,
        count: agg.totalCount,
        datasources: agg.datasources,
        dsToCount: agg.dsToCount,
        parentNodeId: parentId,
        parentEncodedNodeId: parentEncodedId,
      });

      const edgeId = `edge::${parentId}::${direction}::${agg.edgeType}`;
      if (direction === "outgoing") {
        edges.push({ id: edgeId, source: parentId, target: countNodeId, label: agg.edgeType, color: "#cccccc", type: "stem", dashed: true, size: 1.5, direction, edgeType: agg.edgeType, datasources: agg.datasources });
      } else {
        edges.push({ id: edgeId, source: countNodeId, target: parentId, label: agg.edgeType, color: "#cccccc", type: "stem", dashed: true, size: 1.5, direction, edgeType: agg.edgeType, datasources: agg.datasources });
      }
    }
  }
}

/**
 * Push overlapping nodes apart iteratively.
 * Root is fixed.
 */
function resolveCollisions(nodes: LayoutNode[]): void {
  const movable = nodes.filter(n => n.type !== "root");
  if (movable.length < 2) return;

  for (let iter = 0; iter < COLLISION_ITERATIONS; iter++) {
    let moved = false;

    for (let i = 0; i < movable.length; i++) {
      for (let j = i + 1; j < movable.length; j++) {
        const a = movable[i];
        const b = movable[j];

        const dx = b.x - a.x;
        const dy = b.y - a.y;
        const dist = Math.sqrt(dx * dx + dy * dy);
        const minDist = a.size + b.size + COLLISION_PADDING;

        if (dist < minDist && dist > 0) {
          const overlap = (minDist - dist) / 2;
          const nx = dx / dist;
          const ny = dy / dist;

          // Push both nodes apart equally
          a.x -= nx * overlap;
          a.y -= ny * overlap;
          b.x += nx * overlap;
          b.y += ny * overlap;
          moved = true;
        } else if (dist === 0) {
          // Identical positions: nudge randomly
          a.x += (Math.random() - 0.5) * 10;
          a.y += (Math.random() - 0.5) * 10;
          moved = true;
        }
      }
    }

    if (!moved) break;
  }
}
