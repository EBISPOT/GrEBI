import encodeNodeId from "../encodeNodeId";

/**
 * How many values of each property a page loads of a node. A hub node has
 * tens of thousands of values on a few properties, its relationships, and is
 * over 100 MB whole; cut to this it loads at once, and the property view
 * offers the rest. Few other properties have more values than this.
 */
export const NODE_MAX_VALUES = 100;

/**
 * The API path of a node: abridged to NODE_MAX_VALUES values per property
 * (the node then says in grebi:truncated which properties were cut, and has
 * no embedding vectors), or whole.
 */
export function nodePath(graph: string, nodeId: string, options: { lang?: string; whole?: boolean } = {}): string {
  const params: string[] = [];
  if (options.lang) params.push(`lang=${encodeURIComponent(options.lang)}`);
  if (!options.whole) params.push(`maxValues=${NODE_MAX_VALUES}`);
  return `api/v1/graphs/${graph}/nodes/${encodeNodeId(nodeId)}${params.length > 0 ? "?" + params.join("&") : ""}`;
}
