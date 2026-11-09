import rawConfig from "../../../../../configs/ui_configs/graph_view.yaml";

/** A group of edge types that the graph view draws together. */
export interface EdgeTypeCategory {
  id: string;
  label: string;
  /** Its edge types are hidden until they are shown from the filters. */
  hidden: boolean;
  patterns: RegExp[];
}

/** The graph view's settings, from configs/ui_configs/graph_view.yaml. */
export interface GraphViewConfig {
  /**
   * With more stems than this around a node, each category's stems are
   * gathered into one bubble; an open category draws at most this many.
   */
  maxStems: number;
  /** In order: an edge type belongs to the first that matches it, else to `other`. */
  categories: EdgeTypeCategory[];
  /** Where an edge type no category matches goes: the config's "other" category, if it has one. */
  other: EdgeTypeCategory;
  /** Drawn as stems of their own, never gathered into a category. */
  pinned: RegExp[];
  /** Hidden until shown from the filters, whatever their category. */
  hidden: RegExp[];
}

const OTHER: EdgeTypeCategory = { id: "other", label: "Other", hidden: false, patterns: [] };

/** A config that changes nothing: every edge type shown, in one category, never gathered. */
export const PLAIN_GRAPH_VIEW_CONFIG: GraphViewConfig = {
  maxStems: Infinity,
  categories: [],
  other: OTHER,
  pinned: [],
  hidden: [],
};

/** A pattern matches a whole edge type name; * stands for any run of characters, and case is ignored. */
function compilePattern(pattern: string): RegExp {
  const body = pattern
    .split("*")
    .map((part) => part.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"))
    .join(".*");
  return new RegExp(`^${body}$`, "i");
}

function patterns(value: unknown, where: string): RegExp[] {
  if (value === undefined || value === null) return [];
  if (!Array.isArray(value) || value.some((p) => typeof p !== "string" || p === "")) {
    throw new Error(`${where} must be a list of edge type patterns`);
  }
  return value.map(compilePattern);
}

function checkKeys(value: object, allowed: string[], where: string) {
  const unknown = Object.keys(value).filter((k) => !allowed.includes(k));
  if (unknown.length > 0) throw new Error(`${where} has unknown keys: ${unknown.join(", ")}`);
}

/** The config as written in graph_view.yaml; throws, saying what is wrong, if it is malformed. */
export function parseGraphViewConfig(raw: unknown): GraphViewConfig {
  if (!raw || typeof raw !== "object" || Array.isArray(raw)) throw new Error("the graph view config must be a mapping");
  const r = raw as Record<string, any>;
  checkKeys(r, ["max_stems", "pinned_edge_types", "hidden_edge_types", "categories"], "the graph view config");

  const maxStems = r.max_stems ?? Infinity;
  if (typeof maxStems !== "number" || !(maxStems >= 1)) throw new Error("max_stems must be a number of at least 1");

  if (r.categories !== undefined && !Array.isArray(r.categories)) throw new Error("categories must be a list");
  const ids = new Set<string>();
  let other = OTHER;
  const categories: EdgeTypeCategory[] = [];
  (r.categories ?? []).forEach((c: any, i: number) => {
    const where = `category ${i + 1}`;
    if (!c || typeof c !== "object" || Array.isArray(c)) throw new Error(`${where} must be a mapping`);
    checkKeys(c, ["id", "label", "hidden", "edge_types"], where);
    if (typeof c.id !== "string" || c.id === "" || typeof c.label !== "string" || c.label === "") {
      throw new Error(`${where} needs an id and a label`);
    }
    if (ids.has(c.id)) throw new Error(`the category id ${c.id} is used twice`);
    if (c.hidden !== undefined && typeof c.hidden !== "boolean") throw new Error(`category ${c.id}: hidden must be true or false`);
    ids.add(c.id);
    const category = { id: c.id, label: c.label, hidden: c.hidden === true, patterns: patterns(c.edge_types, `category ${c.id}: edge_types`) };
    categories.push(category);
    // a category named "other" also takes the edge types no category matches
    if (c.id === OTHER.id) other = category;
  });

  return {
    maxStems,
    categories,
    other,
    pinned: patterns(r.pinned_edge_types, "pinned_edge_types"),
    hidden: patterns(r.hidden_edge_types, "hidden_edge_types"),
  };
}

const categoryCache = new WeakMap<GraphViewConfig, Map<string, EdgeTypeCategory>>();

/** The category an edge type belongs to: the first that matches it, else `other`. */
export function categoryOf(config: GraphViewConfig, edgeType: string): EdgeTypeCategory {
  let cache = categoryCache.get(config);
  if (!cache) categoryCache.set(config, (cache = new Map()));
  let category = cache.get(edgeType);
  if (!category) {
    category = config.categories.find((c) => c.patterns.some((p) => p.test(edgeType))) ?? config.other;
    cache.set(edgeType, category);
  }
  return category;
}

/** The categories in the order they are drawn and listed: the config's, then `other` unless the config placed it. */
export function orderedCategories(config: GraphViewConfig): EdgeTypeCategory[] {
  return config.categories.includes(config.other) ? config.categories : [...config.categories, config.other];
}

export function isPinned(config: GraphViewConfig, edgeType: string): boolean {
  return config.pinned.some((p) => p.test(edgeType));
}

/** Whether an edge type starts hidden: its category is, or the config lists it. */
export function isHiddenByDefault(config: GraphViewConfig, edgeType: string): boolean {
  return categoryOf(config, edgeType).hidden || config.hidden.some((p) => p.test(edgeType));
}

function loadGraphViewConfig(): GraphViewConfig {
  try {
    return parseGraphViewConfig(rawConfig);
  } catch (e) {
    // the tests check the shipped config; should a broken one get through, the view still works
    console.error("configs/ui_configs/graph_view.yaml could not be used, so the graph view shows every edge type ungrouped", e);
    return PLAIN_GRAPH_VIEW_CONFIG;
  }
}

/** The settings the graph view runs with. */
export const graphViewConfig: GraphViewConfig = loadGraphViewConfig();
