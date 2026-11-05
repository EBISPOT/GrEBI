/**
 * The properties never shown when an entity's properties are listed: the
 * structure of a node or edge (its refs, ids and endpoints, shown in their own
 * ways) and OLS's bookkeeping, which says nothing about the entity itself.
 */
export const HIDDEN_PROPS: ReadonlySet<string> = new Set([
  "_refs",
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
  return HIDDEN_PROPS.has(key);
}
