import { AggregatedEdgeCount, CategoryOpening, ExpandedNodeState, aggregateCounts, categoryKey, expandedKey } from "./useGraphViewState";
import { EdgeTypeCategory, GraphViewConfig, categoryOf, isPinned, orderedCategories } from "./graphViewConfig";
import GraphNodeRef from "../../model/GraphNodeRef";

const MIN_COUNT_NODE_SIZE = 20;
const MAX_COUNT_NODE_SIZE = 60;
const INNER_RADIUS = 250;
const COLLISION_PADDING = 15; // the least space between two nodes
const COLLISION_ITERATIONS = 30; // number of collision resolution passes

/** The smallest span of the layout the view fits to the canvas: the ring of stems around the root. */
const MIN_VIEW_EXTENT = 2 * INNER_RADIUS;

// A stem's label is drawn beside its bubble, on the side away from the
// parent, in text of a fixed size on screen. So that no label runs into
// another or into a bubble, bubbles on the same side of a parent are kept
// apart top to bottom by a row's height: LABEL_ROW_PX on a view VIEW_PX tall
// (the graph area less its padding), which is more layout units the larger
// the ring, and never less than MIN_ROW_GAP. A view that tall has room for
// some 26 such rows on a side whatever the size of the ring, so the height
// asked for stops growing at MAX_ROW_GAP: a node with more stems than that
// gets a ring that holds its bubbles, and its labels show once zoomed in.
const LABEL_ROW_PX = 16;
const VIEW_PX = 480;
const MIN_ROW_GAP = 24;
const MAX_ROW_GAP = 44;
/** The room the view leaves beside the layout for the labels, as a share of the layout's height. */
const LABEL_ROOM = 0.45;
/** The step, in radians, by which a bubble is moved along its arc until it is clear of the ones before it. */
const ARC_STEP = 0.004;

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
  /**
   * What is written beside the node, outside it: for a stem's bubble, the
   * edge type or the category it stands for. On the right of the node or on
   * its left, whichever is away from its parent.
   */
  outerLabel?: string;
  outerLabelSide?: "left" | "right";
  /** A category's label names a group of edge types and not one of them, and is drawn in bold */
  outerLabelBold?: boolean;
  /** The least distance, top to bottom, to the next node labelled on the same side: the room its label has */
  rowGap?: number;
  /**
   * For the root and the expanded nodes: where the name is written in full
   * when it does not fit inside the node. The nodes along a chain of
   * expansions alternate, so that neighbours' names are not on one line.
   */
  namePlacement?: "above" | "below";
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
  /** For a stem that stands for several edge types (to a category or "more" bubble, or to a node several types lead to): those types */
  edgeTypes?: string[];
  datasources?: string[];
  /** The label is drawn along the edge. A stem's is not: it is beside the stem's bubble (the node's outerLabel). */
  labelled: boolean;
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
 *
 * The box also leaves room beside the layout for the labels written outside
 * the nodes (LayoutNode.outerLabel), on each side that has any: they are text
 * of a fixed size on screen, so the room is a share of the layout's height,
 * which is what sets the scale in a view wider than it is tall.
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
  const labelsLeft = layout.nodes.some((n) => n.outerLabel && n.outerLabelSide === "left");
  const labelsRight = layout.nodes.some((n) => n.outerLabel && n.outerLabelSide !== "left");
  if (width >= MIN_VIEW_EXTENT && height >= MIN_VIEW_EXTENT && !labelsLeft && !labelsRight) return null;

  const cx = (minX + maxX) / 2;
  const cy = (minY + maxY) / 2;
  const halfWidth = Math.max(width, MIN_VIEW_EXTENT) / 2;
  const halfHeight = Math.max(height, MIN_VIEW_EXTENT) / 2;
  const labelRoom = LABEL_ROOM * 2 * halfHeight;
  return {
    x: [cx - halfWidth - (labelsLeft ? labelRoom : 0), cx + halfWidth + (labelsRight ? labelRoom : 0)],
    y: [cy - halfHeight, cy + halfHeight],
  };
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
  const rootSize = 40;
  nodes.push({
    id: rootId,
    x: 0,
    y: 0,
    size: rootSize,
    label: rootLabel,
    type: "root",
    color: "#555555",
    labelColor: "#ffffff",
    hasHiddenChildren: rootHasExpansion,
    namePlacement: "below",
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

  layoutFan(
    { id: rootId, encodedId: rootEncodedId, x: 0, y: 0, size: rootSize, depth: 0 },
    incomingAggregated,
    outgoingAggregated,
    // Incoming: left semicircle (PI/2 → 3PI/2) with gap inset
    [Math.PI / 2 + GAP, (3 * Math.PI) / 2 - GAP],
    // Outgoing: right semicircle (PI/2 → -PI/2) with gap inset. Sigma draws y
    // upwards, so both sides list their stems from the top down.
    [Math.PI / 2 - GAP, -Math.PI / 2 + GAP],
    INNER_RADIUS,
    { expandedNodes, dsExclude, hiddenEdgeTypes, autoExpandedNodes, grouping, maxCount, nodes, edges, usedNodeIds },
  );

  // Push apart any nodes of different fans that still overlap
  resolveCollisions(nodes);

  return { nodes, edges };
}

/** What every part of the layout works with: the state it lays out, and the nodes and edges laid out so far. */
interface LayoutContext {
  expandedNodes: Map<string, ExpandedNodeState>;
  dsExclude: Set<string>;
  hiddenEdgeTypes: Set<string>;
  autoExpandedNodes: Map<string, GraphNodeRef>;
  grouping: StemGrouping | undefined;
  maxCount: number;
  nodes: LayoutNode[];
  edges: LayoutEdge[];
  usedNodeIds: Set<string>;
}

/** A node with stems around it. */
interface Parent {
  id: string;
  encodedId: string;
  x: number;
  y: number;
  size: number;
  /** How many expansions from the root */
  depth: number;
}

/** The angles an arc runs from and to; its stems are placed in that order. */
type Arc = [number, number];

/** One thing placed around a parent: a count bubble, a node that a single edge leads to, a category's bubble or its "+N more". */
type Slot =
  | { kind: "count"; agg: AggregatedEdgeCount }
  | { kind: "single"; nodeId: string; node: GraphNodeRef; aggs: AggregatedEdgeCount[] }
  | Exclude<StemGroup, { kind: "stem" }>;

/**
 * Lay out what is around one parent node: its incoming stems on one arc and
 * its outgoing stems on the other.
 *
 * If the parent has an expansion, only the expanded edge is shown (no other
 * children visible). If the parent is a "leaf" (no expansion), all children
 * are shown, with de-duplicated singulars (auto-expanded nodes sharing the
 * same target ID collapse into one graph node, reached by one stem that
 * stands for all the edge types).
 *
 * Since no node can appear twice, every real node uses its actual node ID
 * as the graph ID. Only synthetic count bubbles use a namespaced ID.
 */
function layoutFan(
  parent: Parent,
  incoming: AggregatedEdgeCount[],
  outgoing: AggregatedEdgeCount[],
  incomingArc: Arc,
  outgoingArc: Arc,
  radius: number,
  ctx: LayoutContext,
) {
  // Does this parent have any expansion (in any direction)?
  const hasExpansion = Array.from(ctx.expandedNodes.keys()).some(k => k.startsWith(parent.id + "::"));
  if (hasExpansion) {
    layoutExpansion(parent, incoming, "incoming", incomingArc, radius, ctx);
    layoutExpansion(parent, outgoing, "outgoing", outgoingArc, radius, ctx);
    return;
  }

  // Whether this parent's categories are gathered into bubbles
  const gathered = isGathered(ctx.grouping, incoming, outgoing);
  // in this order: the node an incoming edge leads to is not drawn again for an outgoing one
  const incomingSlots = stemSlots(parent, incoming, "incoming", gathered, ctx);
  const outgoingSlots = stemSlots(parent, outgoing, "outgoing", gathered, ctx);
  if (incomingSlots.length + outgoingSlots.length === 0) return;
  const incomingSizes = incomingSlots.map((slot) => slotSize(slot, ctx.maxCount));
  const outgoingSizes = outgoingSlots.map((slot) => slotSize(slot, ctx.maxCount));

  // The smallest ring, no smaller than asked for, on which both sides fit
  // their arcs with every bubble and every label clear of the others
  let ring = Math.max(radius, parent.size + MAX_COUNT_NODE_SIZE + COLLISION_PADDING);
  let placedIncoming = placeOnArc(incomingSizes, parent, incomingArc, ring, []);
  let placedOutgoing = placedIncoming && placeOnArc(outgoingSizes, parent, outgoingArc, ring, placedIncoming);
  for (let attempt = 0; attempt < 80 && !(placedIncoming && placedOutgoing); attempt++) {
    ring *= 1.1;
    placedIncoming = placeOnArc(incomingSizes, parent, incomingArc, ring, []);
    placedOutgoing = placedIncoming && placeOnArc(outgoingSizes, parent, outgoingArc, ring, placedIncoming);
  }
  // no ring was large enough, which a ring two thousand times the size rules out: place them at even angles
  placedIncoming = placedIncoming || evenlyOnArc(incomingSizes, parent, incomingArc, ring);
  placedOutgoing = placedOutgoing || evenlyOnArc(outgoingSizes, parent, outgoingArc, ring);

  const rowGap = rowGapAt(ring);
  incomingSlots.forEach((slot, i) => addStem(parent, "incoming", slot, placedIncoming![i], rowGap, ctx));
  outgoingSlots.forEach((slot, i) => addStem(parent, "outgoing", slot, placedOutgoing![i], rowGap, ctx));
}

/** The expanded edge of a parent in one direction, the node it leads to, and that node's own stems. */
function layoutExpansion(
  parent: Parent,
  aggregated: AggregatedEdgeCount[],
  direction: "incoming" | "outgoing",
  arc: Arc,
  radius: number,
  ctx: LayoutContext,
) {
  // --- Parent has an expansion: only show the expanded edge for this direction ---
  const expandedAgg = aggregated.filter(agg => {
    const key = expandedKey(parent.id, direction, agg.edgeType);
    return ctx.expandedNodes.has(key);
  });
  if (expandedAgg.length === 0) return; // expansion is in other direction

  // At most one expansion per parent (enforced by expandEdge)
  const agg = expandedAgg[0];
  const expanded = ctx.expandedNodes.get(expandedKey(parent.id, direction, agg.edgeType))!;

  const angle = (arc[0] + arc[1]) / 2;
  const ecx = parent.x + Math.cos(angle) * radius;
  const ecy = parent.y + Math.sin(angle) * radius;

  // Use actual node ID — skip self-references / back-references
  const expNodeId = expanded.node.getNodeId();
  if (ctx.usedNodeIds.has(expNodeId)) return;

  const expHasExpansion = Array.from(ctx.expandedNodes.keys()).some(k => k.startsWith(expNodeId + "::"));
  const expSize = 28;

  ctx.usedNodeIds.add(expNodeId);
  ctx.nodes.push({
    id: expNodeId,
    x: ecx,
    y: ecy,
    size: expSize,
    label: expanded.node.getName(),
    type: "expanded_node",
    color: "#4a90d9",
    labelColor: "#ffffff",
    direction,
    edgeType: agg.edgeType,
    parentNodeId: parent.id,
    parentEncodedNodeId: parent.encodedId,
    hasHiddenChildren: expHasExpansion,
    namePlacement: parent.depth % 2 === 0 ? "above" : "below",
  });

  // Direct edge from parent to expanded node; alone between the two, it carries its own label
  const edge = { id: `edge::${parent.id}::${direction}::${agg.edgeType}`, label: agg.edgeType, color: "#4a90d9", type: "expanded" as const, dashed: false, size: 2, direction, edgeType: agg.edgeType, datasources: agg.datasources, labelled: true };
  ctx.edges.push(direction === "outgoing" ? { ...edge, source: parent.id, target: expNodeId } : { ...edge, source: expNodeId, target: parent.id });

  // Recurse into expanded node's children (if loaded)
  if (!expanded.loading) {
    const subIncoming = aggregateCounts(expanded.incomingEdgeCounts, ctx.dsExclude, ctx.hiddenEdgeTypes);
    const subOutgoing = aggregateCounts(expanded.outgoingEdgeCounts, ctx.dsExclude, ctx.hiddenEdgeTypes);
    const coneSpan = Math.PI * 1.2;
    // each half of the cone lists its stems from its higher end down, as the root's two sides do
    const topDown = (from: number, to: number): Arc => (Math.sin(from) >= Math.sin(to) ? [from, to] : [to, from]);
    layoutFan(
      { id: expNodeId, encodedId: expanded.node.getEncodedNodeId(), x: ecx, y: ecy, size: expSize, depth: parent.depth + 1 },
      subIncoming,
      subOutgoing,
      topDown(angle - coneSpan / 2, angle),
      topDown(angle, angle + coneSpan / 2),
      Math.max(radius * 0.65, 100),
      ctx,
    );
  }
}

/**
 * What is drawn around a parent in one direction, grouped as groupStems says.
 * A count=1 edge already resolved to its node is drawn as that node, and
 * edges of several types to the same node share it.
 */
function stemSlots(
  parent: Parent,
  aggregated: AggregatedEdgeCount[],
  direction: "incoming" | "outgoing",
  gathered: boolean,
  ctx: LayoutContext,
): Slot[] {
  const slots: Slot[] = [];
  const singles = new Map<string, Extract<Slot, { kind: "single" }>>();
  for (const group of groupStems(aggregated, ctx.grouping, gathered, parent.id, direction)) {
    if (group.kind !== "stem") {
      slots.push(group);
      continue;
    }
    const agg = group.agg;
    const autoNode = agg.totalCount === 1 ? ctx.autoExpandedNodes.get(expandedKey(parent.id, direction, agg.edgeType)) : undefined;
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
  const drawn = slots.filter((slot) => slot.kind !== "single" || !ctx.usedNodeIds.has(slot.nodeId));
  for (const slot of drawn) {
    if (slot.kind === "single") ctx.usedNodeIds.add(slot.nodeId);
  }
  return drawn;
}

const totalCount = (aggs: AggregatedEdgeCount[]) => aggs.reduce((n, a) => n + a.totalCount, 0);

function slotSize(slot: Slot, maxCount: number): number {
  switch (slot.kind) {
    case "single": return 28;
    case "count": return countToSize(slot.agg.totalCount, maxCount);
    case "more": return MORE_SIZE;
    case "category": return slot.opening !== null ? OPEN_CATEGORY_SIZE : countToSize(totalCount(slot.aggs), maxCount);
  }
}

/** A bubble placed around its parent: where, how large, and on which side of the parent, which is the side its label is written on. */
interface Disc {
  x: number;
  y: number;
  size: number;
  /** How far right of the parent */
  dx: number;
  right: boolean;
}

function discAt(parent: Parent, angle: number, ring: number, size: number): Disc {
  const dx = Math.cos(angle) * ring;
  return { x: parent.x + dx, y: parent.y + Math.sin(angle) * ring, size, dx, right: dx >= 0 };
}

/** The least distance, top to bottom, between bubbles labelled on the same side of a parent with a ring this large. */
function rowGapAt(ring: number): number {
  return Math.min(MAX_ROW_GAP, Math.max(MIN_ROW_GAP, (LABEL_ROW_PX * 2 * (ring + MAX_COUNT_NODE_SIZE)) / VIEW_PX));
}

/**
 * Whether two bubbles around the same parent are too close: nearer than the
 * padding, or, on the same side of the parent, with too little height
 * between them for their labels. The labels are written outwards, so the
 * label of the bubble nearer the parent runs past the other bubble and must
 * clear it too.
 */
function tooClose(a: Disc, b: Disc, rowGap: number): boolean {
  if (Math.hypot(a.x - b.x, a.y - b.y) < a.size + b.size + COLLISION_PADDING) return true;
  if (a.right !== b.right) return false;
  const outer = Math.abs(a.dx) > Math.abs(b.dx) ? a : b;
  return Math.abs(a.y - b.y) < Math.max(rowGap, outer.size + rowGap / 2);
}

/**
 * Places bubbles of the given sizes along an arc around their parent, in
 * order, clear of each other and of the bubbles already placed around the
 * parent: first each as near the arc's start as the ones before it allow,
 * then spread over the room that leaves. Null when they do not fit on a ring
 * of this size.
 */
function placeOnArc(sizes: number[], parent: Parent, arc: Arc, ring: number, others: Disc[]): Disc[] | null {
  if (sizes.length === 0) return [];
  const [start, end] = arc;
  const dir = end >= start ? 1 : -1;
  const span = Math.abs(end - start);
  const rowGap = rowGapAt(ring);
  // how far inside the arc's ends a bubble must stay for the next arc's bubbles to be clear of it
  const margin = (size: number) => Math.asin(Math.min(1, (size + COLLISION_PADDING / 2) / ring));
  const clear = (disc: Disc, placed: Disc[]) =>
    !placed.some((p) => tooClose(disc, p, rowGap)) && !others.some((o) => tooClose(disc, o, rowGap));

  const turns: number[] = [];
  const packed: Disc[] = [];
  let turn = 0;
  for (const size of sizes) {
    turn = Math.max(turn, margin(size));
    let disc = discAt(parent, start + dir * turn, ring, size);
    while (!clear(disc, packed)) {
      turn += ARC_STEP;
      if (turn > span - margin(size)) return null;
      disc = discAt(parent, start + dir * turn, ring, size);
    }
    if (turn > span - margin(size)) return null;
    turns.push(turn);
    packed.push(disc);
  }

  const room = span - margin(sizes[sizes.length - 1]) - turn;
  const spread: Disc[] = [];
  for (let i = 0; i < sizes.length; i++) {
    const disc = discAt(parent, start + dir * (turns[i] + (room * (i + 1)) / (sizes.length + 1)), ring, sizes[i]);
    // spread out, two bubbles near the top or bottom of the ring can end up level: keep them packed then
    if (!clear(disc, spread)) return packed;
    spread.push(disc);
  }
  return spread;
}

/** Bubbles at even angles along an arc, whether or not they are clear of each other. */
function evenlyOnArc(sizes: number[], parent: Parent, arc: Arc, ring: number): Disc[] {
  const step = (arc[1] - arc[0]) / (sizes.length + 1);
  return sizes.map((size, i) => discAt(parent, arc[0] + step * (i + 1), ring, size));
}

/** The node of one slot at its place, labelled on the side away from the parent, and the stem from the parent to it. */
function addStem(
  parent: Parent,
  direction: "incoming" | "outgoing",
  slot: Slot,
  disc: Disc,
  rowGap: number,
  ctx: LayoutContext,
) {
  const placed = {
    x: disc.x,
    y: disc.y,
    size: disc.size,
    direction,
    parentNodeId: parent.id,
    parentEncodedNodeId: parent.encodedId,
    outerLabelSide: disc.right ? "right" as const : "left" as const,
    rowGap,
  };
  const stemTo = (nodeId: string, id: string, rest: Omit<LayoutEdge, "id" | "source" | "target" | "labelled">) => {
    // the stem's label is beside its bubble, not along the stem, where the stems of a busy node cross
    ctx.edges.push(direction === "outgoing"
      ? { id, source: parent.id, target: nodeId, ...rest, labelled: false }
      : { id, source: nodeId, target: parent.id, ...rest, labelled: false });
  };

  if (slot.kind === "single") {
    // use the real node ID as the graph ID
    const { nodeId, node: autoNode, aggs } = slot;
    const edgeTypes = aggs.map((a) => a.edgeType);
    const datasources = aggs.flatMap(a => a.datasources).filter((v, i, arr) => arr.indexOf(v) === i);
    // the label says where the edge leads, as the node is too small to hold its name: an
    // outgoing edge leads to it, an incoming one comes from it
    const name = autoNode.getName();
    const label = direction === "outgoing" ? `${edgeTypes.join(", ")} \u2192 ${name}` : `${name} \u2192 ${edgeTypes.join(", ")}`;
    ctx.nodes.push({
      ...placed,
      id: nodeId,
      label: name,
      type: "auto_expanded_node",
      color: "#4a90d9",
      labelColor: "#ffffff",
      edgeType: edgeTypes[0],
      ...(edgeTypes.length > 1 ? { edgeTypes } : {}),
      count: 1,
      datasources,
      dsToCount: aggs.reduce<{ [ds: string]: number }>((acc, a) => ({ ...acc, ...a.dsToCount }), {}),
      outerLabel: label,
    });
    // One stem to the node, standing for every edge type that leads to it
    stemTo(nodeId, `edge::${parent.id}::${direction}::${edgeTypes[0]}`, {
      label: edgeTypes.join(", "), color: "#4a90d9", type: "expanded", dashed: false, size: 2, direction,
      edgeType: edgeTypes[0], ...(edgeTypes.length > 1 ? { edgeTypes } : {}), datasources,
    });
  } else if (slot.kind === "count") {
    const agg = slot.agg;
    const countNodeId = `count::${parent.id}::${direction}::${agg.edgeType}`;
    ctx.nodes.push({
      ...placed,
      id: countNodeId,
      label: formatter.format(agg.totalCount),
      type: "count",
      color: "#e8e8e8",
      labelColor: "#666666",
      edgeType: agg.edgeType,
      count: agg.totalCount,
      datasources: agg.datasources,
      dsToCount: agg.dsToCount,
      outerLabel: agg.edgeType,
    });
    stemTo(countNodeId, `edge::${parent.id}::${direction}::${agg.edgeType}`, {
      label: agg.edgeType, color: "#cccccc", type: "stem", dashed: true, size: 1.5, direction, edgeType: agg.edgeType, datasources: agg.datasources,
    });
  } else {
    // a category's bubble, or the "+N more" of an open one
    const { category, aggs } = slot;
    const total = totalCount(aggs);
    const edgeTypes = aggs.map((a) => a.edgeType);
    const datasources = Array.from(new Set(aggs.flatMap((a) => a.datasources)));
    const dsToCount: { [ds: string]: number } = {};
    for (const a of aggs) for (const [ds, n] of Object.entries(a.dsToCount)) dsToCount[ds] = (dsToCount[ds] || 0) + n;
    const open = slot.kind === "category" && slot.opening !== null;
    const id = `${slot.kind}::${parent.id}::${direction}::${category.id}`;
    const label = slot.kind === "more" ? `${aggs.length} more` : `${category.label} · ${aggs.length} types`;
    ctx.nodes.push({
      ...placed,
      id,
      label: slot.kind === "more" ? `+${aggs.length}` : open ? "−" : formatter.format(total),
      type: slot.kind,
      color: open ? CATEGORY_OPEN_COLOR : CATEGORY_COLOR,
      labelColor: CATEGORY_LABEL_COLOR,
      count: total,
      datasources,
      dsToCount,
      edgeTypes,
      categoryId: category.id,
      categoryLabel: category.label,
      ...(slot.kind === "category" ? { open } : {}),
      outerLabel: label,
      outerLabelBold: slot.kind === "category",
    });
    stemTo(id, `edge::${parent.id}::${direction}::${slot.kind}::${category.id}`, {
      label, color: CATEGORY_EDGE_COLOR, type: "stem", dashed: true, size: 1.5, direction, edgeTypes, datasources,
    });
  }
}

/**
 * Push overlapping nodes apart iteratively.
 * Root is fixed. The stems around one node are placed clear of each other, so
 * this only moves nodes of different fans that the chain of expansions
 * brought together.
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

        // (the placement works to the same distance: a hair under it is rounding, not an overlap)
        if (dist < minDist - 1e-6 && dist > 0) {
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
          // Identical positions: nudge them apart, the same way every time
          a.x -= 5;
          a.y -= 3;
          b.x += 5;
          b.y += 3;
          moved = true;
        }
      }
    }

    if (!moved) break;
  }
}
