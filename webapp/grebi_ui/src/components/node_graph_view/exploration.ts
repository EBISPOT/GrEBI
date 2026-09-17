import type { ExpandedNodeState } from "./useGraphViewState";

/** One expansion of the graph view: which edge type was followed, in which direction, to which node. */
export interface ExplorationStep {
  direction: "incoming" | "outgoing";
  edgeType: string;
  nodeId: string;
}

/**
 * What the user has done to the graph view: the chain of expansions from the
 * root (each node has at most one expansion, so it is a chain) and the filters.
 */
export interface Exploration {
  steps: ExplorationStep[];
  excludedDatasources: string[];
  hiddenEdgeTypes: string[];
}

/** The exploration the view is currently showing. */
export function explorationOf(
  rootId: string,
  expandedNodes: Map<string, ExpandedNodeState>,
  dsExclude: Set<string>,
  hiddenEdgeTypes: Set<string>,
): Exploration {
  const steps: ExplorationStep[] = [];
  const seen = new Set<string>();
  let current = rootId;
  while (!seen.has(current)) {
    seen.add(current);
    const next = Array.from(expandedNodes.values()).find((e) => e.parentNodeId === current);
    if (!next) break;
    steps.push({ direction: next.direction, edgeType: next.edgeType, nodeId: next.node.getNodeId() });
    current = next.node.getNodeId();
  }
  return {
    steps,
    excludedDatasources: Array.from(dsExclude).sort(),
    hiddenEdgeTypes: Array.from(hiddenEdgeTypes).sort(),
  };
}

export function isEmptyExploration(e: Exploration): boolean {
  return e.steps.length === 0 && e.excludedDatasources.length === 0 && e.hiddenEdgeTypes.length === 0;
}

/**
 * The exploration as one URL-safe token (base64url of compact JSON), or null
 * when there is nothing worth keeping.
 */
export function serialiseExploration(e: Exploration): string | null {
  if (isEmptyExploration(e)) return null;
  const compact: any = {};
  if (e.steps.length > 0) compact.s = e.steps.map((s) => [s.direction === "incoming" ? "i" : "o", s.edgeType, s.nodeId]);
  if (e.excludedDatasources.length > 0) compact.x = [...e.excludedDatasources].sort();
  if (e.hiddenEdgeTypes.length > 0) compact.h = [...e.hiddenEdgeTypes].sort();
  return toBase64Url(JSON.stringify(compact));
}

/** The exploration a token stands for; null for no token or one that is not ours. */
export function parseExploration(token: string | null | undefined): Exploration | null {
  if (!token) return null;
  try {
    const compact = JSON.parse(fromBase64Url(token));
    if (!compact || typeof compact !== "object") return null;
    const steps: ExplorationStep[] = [];
    for (const s of Array.isArray(compact.s) ? compact.s : []) {
      if (!Array.isArray(s) || s.length !== 3 || (s[0] !== "i" && s[0] !== "o") || typeof s[1] !== "string" || typeof s[2] !== "string") {
        return null;
      }
      steps.push({ direction: s[0] === "i" ? "incoming" : "outgoing", edgeType: s[1], nodeId: s[2] });
    }
    return { steps, excludedDatasources: strings(compact.x), hiddenEdgeTypes: strings(compact.h) };
  } catch (e) {
    return null;
  }
}

function strings(value: any): string[] {
  if (value === undefined || value === null) return [];
  if (!Array.isArray(value) || value.some((v) => typeof v !== "string")) throw new Error("not a list of strings");
  return [...value].sort();
}

function toBase64Url(text: string): string {
  const bytes = new TextEncoder().encode(text);
  let binary = "";
  bytes.forEach((b) => { binary += String.fromCharCode(b); });
  return btoa(binary).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}

function fromBase64Url(token: string): string {
  const base64 = token.replace(/-/g, "+").replace(/_/g, "/");
  const padded = base64 + "=".repeat((4 - (base64.length % 4)) % 4);
  const binary = atob(padded);
  return new TextDecoder().decode(Uint8Array.from(binary, (c) => c.charCodeAt(0)));
}
