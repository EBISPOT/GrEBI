import { useState, useCallback, useRef, useEffect, useMemo } from "react";
import { post } from "../../app/api";
import GraphNodeRef from "../../model/GraphNodeRef";
import { EdgeCountByTypeAndDs, fetchNodeEdgeCounts } from "./edgeCountsCache";
import { GraphViewConfig, graphViewConfig, isHiddenByDefault } from "./graphViewConfig";

export type { EdgeCountByTypeAndDs } from "./edgeCountsCache";

/**
 * Aggregated count for a single edge type (after applying datasource filters).
 */
export interface AggregatedEdgeCount {
  edgeType: string;
  datasources: string[];
  totalCount: number;
  dsToCount: { [ds: string]: number };
}

/**
 * An expanded node with its own edge counts (for recursive expansion).
 */
export interface ExpandedNodeState {
  node: GraphNodeRef;
  edgeType: string;
  direction: "incoming" | "outgoing";
  parentNodeId: string;
  incomingEdgeCounts: EdgeCountByTypeAndDs;
  outgoingEdgeCounts: EdgeCountByTypeAndDs;
  loading: boolean;
}

/**
 * Build an expanded-node map key from parent node ID + direction + edge type.
 */
export function expandedKey(parentNodeId: string, direction: string, edgeType: string): string {
  return `${parentNodeId}::${direction}::${edgeType}`;
}

/**
 * How far a gathered category is opened: "open" draws its largest stems, "all"
 * every one. A category that is not opened stays gathered in one bubble.
 */
export type CategoryOpening = "open" | "all";

/** The key of a category's stems around one parent, in one direction. */
export function categoryKey(parentNodeId: string, direction: string, categoryId: string): string {
  return `${parentNodeId}::${direction}::${categoryId}`;
}

/** Whether an edge type is hidden: as the user set it, else as the config has it at first. */
function hiddenGiven(config: GraphViewConfig, overrides: Map<string, boolean>, edgeType: string): boolean {
  const shown = overrides.get(edgeType);
  return shown === undefined ? isHiddenByDefault(config, edgeType) : !shown;
}

export function aggregateCounts(
  edgeCounts: EdgeCountByTypeAndDs,
  dsExclude: Set<string>,
  hiddenEdgeTypes: Set<string>
): AggregatedEdgeCount[] {
  const result: AggregatedEdgeCount[] = [];
  for (const edgeType of Object.keys(edgeCounts)) {
    if (hiddenEdgeTypes.has(edgeType)) continue;
    const dsToCount = edgeCounts[edgeType];
    const datasources = Object.keys(dsToCount);
    let totalCount = 0;
    for (const ds of datasources) {
      if (!dsExclude.has(ds)) {
        totalCount += dsToCount[ds];
      }
    }
    if (totalCount > 0) {
      result.push({ edgeType, datasources, totalCount, dsToCount });
    }
  }
  result.sort((a, b) => b.totalCount - a.totalCount);
  return result;
}

/**
 * Recursively remove all expansion entries that are descendants of the
 * given node from the map (mutates in place).
 */
function removeExpansionDescendants(map: Map<string, ExpandedNodeState>, nodeId: string, visited?: Set<string>): void {
  const seen = visited || new Set<string>();
  if (seen.has(nodeId)) return;
  seen.add(nodeId);
  const prefix = nodeId + "::";
  for (const [k, v] of Array.from(map.entries())) {
    if (k.startsWith(prefix)) {
      map.delete(k);
      removeExpansionDescendants(map, v.node.getNodeId(), seen);
    }
  }
}

export default function useGraphViewState(graph: string, config: GraphViewConfig = graphViewConfig) {
  const [root, setRoot] = useState<GraphNodeRef | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const [incomingEdgeCounts, setIncomingEdgeCounts] = useState<EdgeCountByTypeAndDs>({});
  const [outgoingEdgeCounts, setOutgoingEdgeCounts] = useState<EdgeCountByTypeAndDs>({});

  const [allDatasources, setAllDatasources] = useState<string[]>([]);
  const [dsExclude, setDsExclude] = useState<Set<string>>(new Set());
  // The user's changes to which edge types the config shows at first: true
  // for shown, false for hidden. Only changes are kept, so that is all the URL records.
  const [edgeTypeOverrides, setEdgeTypeOverrides] = useState<Map<string, boolean>>(new Map());
  // How far the user has opened each gathered category, by categoryKey
  const [openCategories, setOpenCategories] = useState<Map<string, CategoryOpening>>(new Map());

  const [expandedNodes, setExpandedNodes] = useState<Map<string, ExpandedNodeState>>(new Map());

  // Auto-expanded nodes: count=1 edges resolved to their actual node
  // Keyed by expandedKey(parentNodeId, direction, edgeType)
  const [autoExpandedNodes, setAutoExpandedNodes] = useState<Map<string, GraphNodeRef>>(new Map());

  // Track which root we loaded to avoid stale updates
  const loadIdRef = useRef(0);

  const loadEdgeCounts = useCallback(
    async (node: GraphNodeRef) => {
      const myLoadId = ++loadIdRef.current;
      setRoot(node);
      setLoading(true);
      setError(null);
      setExpandedNodes(new Map());
      setAutoExpandedNodes(new Map());
      setEdgeTypeOverrides(new Map());
      setOpenCategories(new Map());

      try {
        const bothCounts = await fetchNodeEdgeCounts(graph, node.getEncodedNodeId());

        if (myLoadId !== loadIdRef.current) return;

        const incoming = bothCounts?.incoming || {};
        const outgoing = bothCounts?.outgoing || {};

        setIncomingEdgeCounts(incoming);
        setOutgoingEdgeCounts(outgoing);

        const dsSet = new Set<string>();
        for (const edgeCounts of [incoming, outgoing]) {
          if (!edgeCounts) continue;
          for (const edgeType of Object.keys(edgeCounts)) {
            for (const ds of Object.keys(edgeCounts[edgeType])) {
              dsSet.add(ds);
            }
          }
        }
        setAllDatasources(Array.from(dsSet).sort());
        setDsExclude(new Set());

        // Resolve count=1 edges for root before showing the graph
        const inAgg = aggregateCounts(incoming || {}, new Set(), new Set());
        const outAgg = aggregateCounts(outgoing || {}, new Set(), new Set());
        const toResolveRoot: Array<{ direction: "incoming" | "outgoing"; edgeType: string }> = [];
        for (const agg of inAgg) {
          if (agg.totalCount === 1) {
            toResolveRoot.push({ direction: "incoming", edgeType: agg.edgeType });
          }
        }
        for (const agg of outAgg) {
          if (agg.totalCount === 1) {
            toResolveRoot.push({ direction: "outgoing", edgeType: agg.edgeType });
          }
        }

        if (toResolveRoot.length > 0) {
          try {
            const resolved = await post<typeof toResolveRoot, { [key: string]: any }>(
              `api/v1/graphs/${graph}/nodes/${node.getEncodedNodeId()}/resolve_single_edges`,
              {},
              toResolveRoot,
            );
            if (myLoadId !== loadIdRef.current) return;
            if (resolved) {
              const newAuto = new Map<string, GraphNodeRef>();
              for (const [respKey, nodeProps] of Object.entries(resolved)) {
                if (!nodeProps) continue;
                const sepIdx = respKey.indexOf("::");
                const dir = respKey.substring(0, sepIdx);
                const et = respKey.substring(sepIdx + 2);
                const autoKey = expandedKey(node.getNodeId(), dir, et);
                newAuto.set(autoKey, new GraphNodeRef(nodeProps));
              }
              setAutoExpandedNodes(newAuto);
            }
          } catch (e) {
            console.error("Failed to resolve root single edges", e);
          }
        }
      } catch (e: any) {
        if (myLoadId !== loadIdRef.current) return;
        setError(e.message || "Failed to load edge counts");
      } finally {
        if (myLoadId === loadIdRef.current) {
          setLoading(false);
        }
      }
    },
    [graph]
  );

  const expandEdge = useCallback(
    async (parentNodeId: string, direction: "incoming" | "outgoing", edgeType: string, node: GraphNodeRef) => {
      const key = expandedKey(parentNodeId, direction, edgeType);

      // Enforce one expansion per parent: clear any existing expansions
      // on this parent (and all their descendants) before adding the new one.
      setExpandedNodes((prev) => {
        const next = new Map(prev);
        const parentPrefix = parentNodeId + "::";
        for (const [k, v] of Array.from(next.entries())) {
          if (k.startsWith(parentPrefix)) {
            removeExpansionDescendants(next, v.node.getNodeId());
            next.delete(k);
          }
        }
        next.set(key, {
          node, edgeType, direction, parentNodeId,
          incomingEdgeCounts: {}, outgoingEdgeCounts: {},
          loading: true,
        });
        return next;
      });

      // Load edge counts for the expanded node
      try {
        const bothCounts = await fetchNodeEdgeCounts(graph, node.getEncodedNodeId());

        const incoming = bothCounts?.incoming || {};
        const outgoing = bothCounts?.outgoing || {};

        setExpandedNodes((prev) => {
          const next = new Map(prev);
          const existing = next.get(key);
          if (existing && existing.node.getNodeId() === node.getNodeId()) {
            next.set(key, {
              ...existing,
              incomingEdgeCounts: incoming || {},
              outgoingEdgeCounts: outgoing || {},
              loading: false,
            });
          }
          return next;
        });

        // Merge any new datasources
        setAllDatasources((prev) => {
          const dsSet = new Set(prev);
          for (const edgeCounts of [incoming, outgoing]) {
            if (!edgeCounts) continue;
            for (const et of Object.keys(edgeCounts)) {
              for (const ds of Object.keys(edgeCounts[et])) {
                dsSet.add(ds);
              }
            }
          }
          const sorted = Array.from(dsSet).sort();
          return sorted.length === prev.length ? prev : sorted;
        });
      } catch (e) {
        console.error("Failed to load edge counts for expanded node", e);
        setExpandedNodes((prev) => {
          const next = new Map(prev);
          const existing = next.get(key);
          if (existing) next.set(key, { ...existing, loading: false });
          return next;
        });
      }
    },
    [graph]
  );

  const collapseEdge = useCallback(
    (parentNodeId: string, direction: "incoming" | "outgoing", edgeType: string) => {
      const key = expandedKey(parentNodeId, direction, edgeType);
      setExpandedNodes((prev) => {
        const next = new Map(prev);
        const expandedNode = next.get(key);
        if (expandedNode) {
          removeExpansionDescendants(next, expandedNode.node.getNodeId());
        }
        next.delete(key);
        return next;
      });
    },
    []
  );

  /** Collapse all expansions below a given node ("rewind" to it). */
  const collapseDescendants = useCallback(
    (nodeId: string) => {
      setExpandedNodes((prev) => {
        const next = new Map(prev);
        removeExpansionDescendants(next, nodeId);
        return next;
      });
    },
    []
  );

  const toggleDsExclude = useCallback(
    (dsEnabled: string[]) => {
      const newExclude = new Set<string>();
      for (const ds of allDatasources) {
        if (!dsEnabled.includes(ds)) {
          newExclude.add(ds);
        }
      }
      setDsExclude(newExclude);
    },
    [allDatasources]
  );


  /** The overrides with the given edge types shown or hidden, keeping only changes from the config's defaults. */
  const withEdgeTypesHidden = useCallback(
    (prev: Map<string, boolean>, edgeTypes: Iterable<string>, hidden: boolean) => {
      const next = new Map(prev);
      for (const t of edgeTypes) {
        if (hidden === isHiddenByDefault(config, t)) next.delete(t);
        else next.set(t, !hidden);
      }
      return next;
    },
    [config]
  );

  const setEdgeTypesHidden = useCallback(
    (edgeTypes: string[], hidden: boolean) => {
      setEdgeTypeOverrides((prev) => withEdgeTypesHidden(prev, edgeTypes, hidden));
    },
    [withEdgeTypesHidden]
  );

  const toggleEdgeTypeHidden = useCallback(
    (edgeType: string) => {
      setEdgeTypeOverrides((prev) => withEdgeTypesHidden(prev, [edgeType], !hiddenGiven(config, prev, edgeType)));
    },
    [config, withEdgeTypesHidden]
  );

  /** The root's edge types: the ones the filters list. */
  const rootEdgeTypes = useCallback(
    () => [...Object.keys(incomingEdgeCounts), ...Object.keys(outgoingEdgeCounts)],
    [incomingEdgeCounts, outgoingEdgeCounts]
  );

  const showAllEdgeTypes = useCallback(() => {
    setEdgeTypesHidden(rootEdgeTypes(), false);
  }, [rootEdgeTypes, setEdgeTypesHidden]);

  /** Both filters at once, when an exploration is restored. */
  const setFilters = useCallback(
    (excludedDatasources: string[], hidden: string[], shown: string[] = []) => {
      setDsExclude(new Set(excludedDatasources));
      setEdgeTypeOverrides(withEdgeTypesHidden(withEdgeTypesHidden(new Map(), hidden, true), shown, false));
    },
    [withEdgeTypesHidden]
  );

  const hideAllEdgeTypes = useCallback(() => {
    setEdgeTypesHidden(rootEdgeTypes(), true);
  }, [rootEdgeTypes, setEdgeTypesHidden]);

  /** Opens a gathered category around a parent a step further, or gathers it back with null. */
  const setCategoryOpening = useCallback(
    (parentNodeId: string, direction: "incoming" | "outgoing", categoryId: string, opening: CategoryOpening | null) => {
      setOpenCategories((prev) => {
        const next = new Map(prev);
        const key = categoryKey(parentNodeId, direction, categoryId);
        if (opening) next.set(key, opening);
        else next.delete(key);
        return next;
      });
    },
    []
  );

  // Every edge type the view knows of, the root's and its expanded nodes'
  const knownEdgeTypes = useMemo(() => {
    const types = new Set<string>();
    const add = (counts: EdgeCountByTypeAndDs) => Object.keys(counts).forEach((t) => types.add(t));
    add(incomingEdgeCounts);
    add(outgoingEdgeCounts);
    for (const e of expandedNodes.values()) {
      add(e.incomingEdgeCounts);
      add(e.outgoingEdgeCounts);
    }
    return types;
  }, [incomingEdgeCounts, outgoingEdgeCounts, expandedNodes]);

  // The edge types hidden now, by the user or by the config
  const hiddenEdgeTypes = useMemo(
    () => new Set(Array.from(knownEdgeTypes).filter((t) => hiddenGiven(config, edgeTypeOverrides, t))),
    [knownEdgeTypes, edgeTypeOverrides, config]
  );

  // Memoised so that they, and the layout computed from them, are the same
  // objects across renders that change nothing here, such as a hover: a new
  // layout makes the renderer rebuild the graph and reset the camera.
  const incomingAggregated = useMemo(
    () => aggregateCounts(incomingEdgeCounts, dsExclude, hiddenEdgeTypes),
    [incomingEdgeCounts, dsExclude, hiddenEdgeTypes]
  );
  const outgoingAggregated = useMemo(
    () => aggregateCounts(outgoingEdgeCounts, dsExclude, hiddenEdgeTypes),
    [outgoingEdgeCounts, dsExclude, hiddenEdgeTypes]
  );

  // Resolve count=1 edges to their actual connected node.
  // Fires whenever the root, filters, or expanded nodes change.
  useEffect(() => {
    if (!root || loading) return;

    // Collect all (parentNodeId, encodedNodeId, direction, edgeType) pairs with count=1
    const toResolve: Array<{
      parentNodeId: string;
      encodedNodeId: string;
      direction: "incoming" | "outgoing";
      edgeType: string;
    }> = [];

    // Root node's own edges
    for (const agg of incomingAggregated) {
      const key = expandedKey(root.getNodeId(), "incoming", agg.edgeType);
      if (agg.totalCount === 1 && !expandedNodes.has(key) && !autoExpandedNodes.has(key)) {
        toResolve.push({
          parentNodeId: root.getNodeId(),
          encodedNodeId: root.getEncodedNodeId(),
          direction: "incoming",
          edgeType: agg.edgeType,
        });
      }
    }
    for (const agg of outgoingAggregated) {
      const key = expandedKey(root.getNodeId(), "outgoing", agg.edgeType);
      if (agg.totalCount === 1 && !expandedNodes.has(key) && !autoExpandedNodes.has(key)) {
        toResolve.push({
          parentNodeId: root.getNodeId(),
          encodedNodeId: root.getEncodedNodeId(),
          direction: "outgoing",
          edgeType: agg.edgeType,
        });
      }
    }

    // Expanded nodes' edges: resolve count=1 edges that aren't already expanded
    for (const [, expState] of expandedNodes) {
      if (expState.loading) continue;
      const expNodeId = expState.node.getNodeId();
      const expEncodedId = expState.node.getEncodedNodeId();
      const expIncoming = aggregateCounts(expState.incomingEdgeCounts, dsExclude, hiddenEdgeTypes);
      const expOutgoing = aggregateCounts(expState.outgoingEdgeCounts, dsExclude, hiddenEdgeTypes);
      for (const agg of expIncoming) {
        const key = expandedKey(expNodeId, "incoming", agg.edgeType);
        if (agg.totalCount === 1 && !expandedNodes.has(key) && !autoExpandedNodes.has(key)) {
          toResolve.push({
            parentNodeId: expNodeId,
            encodedNodeId: expEncodedId,
            direction: "incoming",
            edgeType: agg.edgeType,
          });
        }
      }
      for (const agg of expOutgoing) {
        const key = expandedKey(expNodeId, "outgoing", agg.edgeType);
        if (agg.totalCount === 1 && !expandedNodes.has(key) && !autoExpandedNodes.has(key)) {
          toResolve.push({
            parentNodeId: expNodeId,
            encodedNodeId: expEncodedId,
            direction: "outgoing",
            edgeType: agg.edgeType,
          });
        }
      }
    }

    if (toResolve.length === 0) return;

    // Group by parentNodeId+encodedNodeId so we can batch per node
    const byParent = new Map<string, typeof toResolve>();
    for (const item of toResolve) {
      const key = item.encodedNodeId;
      if (!byParent.has(key)) byParent.set(key, []);
      byParent.get(key)!.push(item);
    }

    // Fire one request per parent node (typically just 1-2 nodes)
    const promises = Array.from(byParent.entries()).map(async ([encodedNodeId, items]) => {
      try {
        const body = items.map((i) => ({
          direction: i.direction,
          edgeType: i.edgeType,
        }));
        const resolved = await post<typeof body, { [key: string]: any }>(
          `api/v1/graphs/${graph}/nodes/${encodedNodeId}/resolve_single_edges`,
          {},
          body,
        );
        if (!resolved) return [];
        // Map response keys ("direction::edgeType") to expandedKey format
        const parentNodeId = items[0].parentNodeId;
        const entries: Array<[string, GraphNodeRef]> = [];
        for (const [respKey, nodeProps] of Object.entries(resolved)) {
          if (!nodeProps) continue;
          const sepIdx = respKey.indexOf("::");
          const dir = respKey.substring(0, sepIdx);
          const et = respKey.substring(sepIdx + 2);
          const autoKey = expandedKey(parentNodeId, dir, et);
          entries.push([autoKey, new GraphNodeRef(nodeProps)]);
        }
        return entries;
      } catch (e) {
        console.error("Failed to resolve single edges", e);
        return [];
      }
    });

    Promise.all(promises).then((results) => {
      const newEntries = results.flat();
      if (newEntries.length === 0) return;
      setAutoExpandedNodes((prev) => {
        const next = new Map(prev);
        for (const [key, node] of newEntries) {
          next.set(key, node);
        }
        return next;
      });
    });
  }, [
    root?.getNodeId(),
    loading,
    graph,
    // Use JSON-serialized keys to detect changes in aggregated results
    incomingAggregated.map((a) => `${a.edgeType}:${a.totalCount}`).join(","),
    outgoingAggregated.map((a) => `${a.edgeType}:${a.totalCount}`).join(","),
    // Re-run when expanded nodes change (new nodes may need resolution)
    Array.from(expandedNodes.keys()).sort().join(","),
    // Re-run when expanded nodes finish loading
    Array.from(expandedNodes.values()).map((e) => `${e.loading}`).join(","),
  ]);

  // True if any expanded node is still loading its edge counts
  const anyExpansionLoading = Array.from(expandedNodes.values()).some(e => e.loading);

  return {
    root,
    loading,
    error,
    incomingEdgeCounts,
    outgoingEdgeCounts,
    allDatasources,
    dsExclude,
    hiddenEdgeTypes,
    edgeTypeOverrides,
    openCategories,
    config,
    expandedNodes,
    incomingAggregated,
    outgoingAggregated,
    autoExpandedNodes,
    anyExpansionLoading,
    loadEdgeCounts,
    expandEdge,
    collapseEdge,
    collapseDescendants,
    toggleDsExclude,
    toggleEdgeTypeHidden,
    setEdgeTypesHidden,
    showAllEdgeTypes,
    hideAllEdgeTypes,
    setFilters,
    setCategoryOpening,
  };
}
