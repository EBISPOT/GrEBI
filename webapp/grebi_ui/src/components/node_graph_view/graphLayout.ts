import { AggregatedEdgeCount, CategoryOpening, ExpandedNodeState, aggregateCounts, categoryKey, expandedKey } from "./useGraphViewState";
import { EdgeTypeCategory, GraphViewConfig, categoryOf, isPinned, orderedCategories } from "./graphViewConfig";
import GraphNodeRef from "../../model/GraphNodeRef";

const MIN_COUNT_NODE_SIZE = 20;
const MAX_COUNT_NODE_SIZE = 60;
const INNER_RADIUS = 250;
const COLLISION_PADDING = 15; // extra padding between nodes during collision resolution
const COLLISION_ITERATIONS = 30; // number of collision resolution passes

/** The smallest span of the layout the view fits to the canvas: the ring of stems around the root. */
const MIN_VIEW_EXTENT = 2 * INNER_RADIUS;

// A category's bubble, its "+N more" bubble, and the stems to them
const CATEGORY_COLOR = "#ede7f6";
const CATEGORY_OPEN_COLOR = "#d1c4e9";
const CATEGORY_LABEL_COLOR = "#4527a0";
const CATEGORY_EDGE_COLOR = "#b39ddb";
const OPEN_CATEGORY_SIZE = 16;
const MORE_SIZE = 20;

export interface LayoutNode {
  id: string;
  x: number;
  y: number;
  size: number;
  label: string;
  /** A category bubble stands for several edge types' stems; "more" for the rest of an open category's */
  type: "root" | "count" | "expanded_node" | "auto_expanded_node" | "category" | "more";
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
  /** For a category or "more" bubble: its category, and the edge types it stands for */
  categoryId?: string;
  categoryLabel?: string;
  edgeTypes?: string[];
  /** For a category bubble: whether its stems are drawn (it then gathers them back when clicked) */
  open?: boolean;
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
  /** For the stem to a category or "more" bubble: the edge types it stands for */
  edgeTypes?: string[];
  datasources?: string[];
}

export interface GraphLayout {
  nodes: LayoutNode[];
  edges: LayoutEdge[];
}

/** A box in layout coordinates, in the shape sigma's setCustomBBox takes. */
export interface LayoutBBox {
  x: [number, number];
  y: [number, number];
}

/**
 * The box the view should fit to the canvas, or null to fit the layout itself.
 *
 * Node sizes are in layout units and the renderer fits the layout's extent to
 * the canvas, so the smaller the layout, the larger its nodes are drawn: the
 * root on its own, when every edge type is hidden, has no extent at all, and
 * sigma then takes it to be one unit wide. The view is fitted to a box at least
 * the size of the ring of stems each way instead, around the layout's centre,
 * so nodes are never drawn larger than at the scale of the ring.
 */
export function viewBBox(layout: GraphLayout): LayoutBBox | null {
  if (layout.nodes.length === 0) return null;

  let minX = Infinity, maxX = -Infinity, minY = Infinity, maxY = -Infinity;
  for (const n of layout.nodes) {
    minX = Math.min(minX, n.x);
    maxX = Math.max(maxX, n.x);
    minY = Math.min(minY, n.y);
    maxY = Math.max(maxY, n.y);
  }
  const width = maxX - minX;
  const height = maxY - minY;
  if (width >= MIN_VIEW_EXTENT && height >= MIN_VIEW_EXTENT) return null;

  const cx = (minX + maxX) / 2;
  const cy = (minY + maxY) / 2;
  const halfWidth = Math.max(width, MIN_VIEW_EXTENT) / 2;
  const halfHeight = Math.max(height, MIN_VIEW_EXTENT) / 2;
  return { x: [cx - halfWidth, cx + halfWidth], y: [cy - halfHeight, cy + halfHeight] };
}

const formatter = Intl.NumberFormat("en", { notation: "compact" });

function countToSize(count: number, maxCount: number): number {
  if (maxCount <= 0) return MIN_COUNT_NODE_SIZE;
  const ratio = Math.min(count / maxCount, 1);
  return MIN_COUNT_NODE_SIZE + ratio * (MAX_COUNT_NODE_SIZE - MIN_COUNT_NODE_SIZE);
}

/** How the stems around a node are grouped: the config, and which categories the user opened. */
export interface StemGrouping {
  config: GraphViewConfig;
  /** How far each category is opened, by categoryKey(parent, direction, category) */
  openCategories: Map<string, CategoryOpening>;
}

/** One thing drawn around a parent in one direction: a stem, a category's bubble, or its "+N more". */
export type StemGroup =
  | { kind: "stem"; agg: AggregatedEdgeCount }
  | { kind: "category"; category: EdgeTypeCategory; aggs: AggregatedEdgeCount[]; opening: CategoryOpening | null }
  | { kind: "more"; category: EdgeTypeCategory; aggs: AggregatedEdgeCount[] };

/**
 * What to draw around a parent in one direction, in order. Stems are grouped
 * by category, in the config's order, largest first within each. When the
 * parent has more stems than the config's max_stems (`gathered`), each
 * category's stems are gathered into one bubble, except pinned edge types and
 * a category with a single stem; an opened category draws its bubble (to
 * gather them back) and then its largest max_stems stems, with the rest in a
 * "+N more" bubble until that is opened too.
 */
export function groupStems(
  aggregated: AggregatedEdgeCount[],
  grouping: StemGrouping | undefined,
  gathered: boolean,
  parentId: string,
  direction: "incoming" | "outgoing",
): StemGroup[] {
  if (!grouping) return aggregated.map((agg) => ({ kind: "stem", agg }));
  const { config, openCategories } = grouping;

  const byCategory = new Map<EdgeTypeCategory, AggregatedEdgeCount[]>();
  for (const agg of [...aggregated].sort((a, b) => b.totalCount - a.totalCount)) {
    const category = categoryOf(config, agg.edgeType);
    if (!byCategory.has(category)) byCategory.set(category, []);
    byCategory.get(category)!.push(agg);
  }

  const groups: StemGroup[] = [];
  const stem = (agg: AggregatedEdgeCount) => groups.push({ kind: "stem", agg });
  for (const category of orderedCategories(config)) {
    const aggs = byCategory.get(category);
    if (!aggs) continue;
    if (!gathered) {
      aggs.forEach(stem);
      continue;
    }
    const rest = aggs.filter((agg) => !isPinned(config, agg.edgeType));
    aggs.filter((agg) => isPinned(config, agg.edgeType)).forEach(stem);
    if (rest.length <= 1) {
      rest.forEach(stem);
      continue;
    }
    const opening = openCategories.get(categoryKey(parentId, direction, category.id)) ?? null;
    groups.push({ kind: "category", category, aggs: rest, opening });
    if (!opening) continue;
    const shown = opening === "all" ? rest : rest.slice(0, config.maxStems);
    shown.forEach(stem);
    if (shown.length < rest.length) groups.push({ kind: "more", category, aggs: rest.slice(shown.length) });
  }
  return groups;
}

/** Whether a parent with these stems has its categories gathered into bubbles. */
function isGathered(grouping: StemGrouping | undefined, ...aggregated: AggregatedEdgeCount[][]): boolean {
  return !!grouping && aggregated.reduce((n, a) => n + a.length, 0) > grouping.config.maxStems;
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
  /** Without it, every stem is drawn, ungrouped */
  grouping?: StemGrouping,
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
  const gathered = isGathered(grouping, incomingAggregated, outgoingAggregated);

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
    grouping, gathered,
  );

  // Outgoing: right semicircle (PI/2 → -PI/2) with gap inset. Sigma draws y
  // upwards, so both sides list their stems from the top down.
  layoutChildren(
    rootId, rootEncodedId,
    0, 0,
    outgoingAggregated, "outgoing",
    Math.PI / 2 - GAP, -Math.PI / 2 + GAP,
    INNER_RADIUS,
    expandedNodes, dsExclude, hiddenEdgeTypes,
    nodes, edges, usedNodeIds, maxCount, 0, autoExpandedNodes,
    grouping, gathered,
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
  grouping: StemGrouping | undefined,
  /** Whether this parent's categories are gathered into bubbles */
  gathered: boolean,
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
      const subGathered = isGathered(grouping, subIncoming, subOutgoing);

      if (subIncoming.length > 0) {
        layoutChildren(
          expNodeId, expanded.node.getEncodedNodeId(),
          ecx, ecy,
          subIncoming, "incoming",
          subMid - coneSpan / 2, subMid,
          nextRadius,
          expandedNodes, dsExclude, hiddenEdgeTypes,
          nodes, edges, usedNodeIds, maxCount, depth + 1, autoExpandedNodes,
          grouping, subGathered,
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
          grouping, subGathered,
        );
      }
    }
  } else {
    // --- No expansion: show the children, grouped as groupStems says ---
    // A count=1 edge already resolved to its node is drawn as that node, and
    // edges of several types to the same node share it, one edge per type.
    type Slot =
      | { kind: "count"; agg: AggregatedEdgeCount }
      | { kind: "single"; nodeId: string; node: GraphNodeRef; aggs: AggregatedEdgeCount[] }
      | Exclude<StemGroup, { kind: "stem" }>;
    const slots: Slot[] = [];
    const singles = new Map<string, Extract<Slot, { kind: "single" }>>();
    for (const group of groupStems(aggregated, grouping, gathered, parentId, direction)) {
      if (group.kind !== "stem") {
        slots.push(group);
        continue;
      }
      const agg = group.agg;
      const autoNode = agg.totalCount === 1 ? autoExpandedNodes.get(expandedKey(parentId, direction, agg.edgeType)) : undefined;
      if (!autoNode) {
        slots.push({ kind: "count", agg });
        continue;
      }
      const nodeId = autoNode.getNodeId();
      const single = singles.get(nodeId);
      if (single) {
        single.aggs.push(agg);
      } else {
        const slot = { kind: "single" as const, nodeId, node: autoNode, aggs: [agg] };
        singles.set(nodeId, slot);
        slots.push(slot);
      }
    }

    // Skip self-references (edges pointing back to an already-visible node)
    const placed = slots.filter((slot) => slot.kind !== "single" || !usedNodeIds.has(slot.nodeId));
    if (placed.length === 0) return;
    const angleStep = (endAngle - startAngle) / (placed.length + 1);

    const stemTo = (nodeId: string, id: string, rest: Omit<LayoutEdge, "id" | "source" | "target">) => {
      edges.push(direction === "outgoing"
        ? { id, source: parentId, target: nodeId, ...rest }
        : { id, source: nodeId, target: parentId, ...rest });
    };

    placed.forEach((slot, i) => {
      const angle = startAngle + angleStep * (i + 1);
      const cx = parentX + Math.cos(angle) * radius;
      const cy = parentY + Math.sin(angle) * radius;

      if (slot.kind === "single") {
        // use the real node ID as the graph ID
        const { nodeId, node: autoNode, aggs } = slot;
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
          stemTo(nodeId, `edge::${parentId}::${direction}::${agg.edgeType}`, {
            label: agg.edgeType, color: "#4a90d9", type: "expanded", dashed: false, size: 2, direction, edgeType: agg.edgeType, datasources: agg.datasources,
          });
        }
      } else if (slot.kind === "count") {
        const agg = slot.agg;
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
        stemTo(countNodeId, `edge::${parentId}::${direction}::${agg.edgeType}`, {
          label: agg.edgeType, color: "#cccccc", type: "stem", dashed: true, size: 1.5, direction, edgeType: agg.edgeType, datasources: agg.datasources,
        });
      } else {
        // a category's bubble, or the "+N more" of an open one
        const { category, aggs } = slot;
        const total = aggs.reduce((n, a) => n + a.totalCount, 0);
        const edgeTypes = aggs.map((a) => a.edgeType);
        const datasources = Array.from(new Set(aggs.flatMap((a) => a.datasources)));
        const dsToCount: { [ds: string]: number } = {};
        for (const a of aggs) for (const [ds, n] of Object.entries(a.dsToCount)) dsToCount[ds] = (dsToCount[ds] || 0) + n;
        const open = slot.kind === "category" && slot.opening !== null;
        const id = `${slot.kind}::${parentId}::${direction}::${category.id}`;
        nodes.push({
          id,
          x: cx,
          y: cy,
          size: slot.kind === "more" ? MORE_SIZE : open ? OPEN_CATEGORY_SIZE : countToSize(total, maxCount),
          label: slot.kind === "more" ? `+${aggs.length}` : open ? "\u2212" : formatter.format(total),
          type: slot.kind,
          color: open ? CATEGORY_OPEN_COLOR : CATEGORY_COLOR,
          labelColor: CATEGORY_LABEL_COLOR,
          direction,
          count: total,
          datasources,
          dsToCount,
          edgeTypes,
          categoryId: category.id,
          categoryLabel: category.label,
          ...(slot.kind === "category" ? { open } : {}),
          parentNodeId: parentId,
          parentEncodedNodeId: parentEncodedId,
        });
        stemTo(id, `edge::${parentId}::${direction}::${slot.kind}::${category.id}`, {
          label: slot.kind === "more" ? `${aggs.length} more` : `${category.label} \u00b7 ${aggs.length} types`,
          color: CATEGORY_EDGE_COLOR, type: "stem", dashed: true, size: 1.5, direction, edgeTypes, datasources,
        });
      }
    });
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
