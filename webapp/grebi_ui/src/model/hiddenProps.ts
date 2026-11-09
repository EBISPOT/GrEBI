/**
 * The properties never shown when an entity's properties are listed: the
 * structure of a node or edge (its refs, ids and endpoints, shown in their own
 * ways, and the count of what a node loaded abridged leaves out) and OLS's
 * bookkeeping, which says nothing about the entity itself. A node's embedding
 * vectors, hundreds of numbers each, are not listed either; a node loaded
 * abridged does not carry them.
 */
export const HIDDEN_PROPS: ReadonlySet<string> = new Set([
  "_refs",
  "grebi:truncated",
  "id",
  "grebi:nodeId",
  "grebi:edgeId",
  "grebi:fromNodeId",
  "grebi:toNodeId",
  "from",
  "to",
  "ols:definitionProperty",
  "ols:isPreferredRoot",
]);

export function isHiddenProp(key: string): boolean {
  return HIDDEN_PROPS.has(key) || key.startsWith("embedding:");
}
