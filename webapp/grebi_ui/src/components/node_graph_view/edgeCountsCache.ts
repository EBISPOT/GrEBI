import { get } from "../../app/api";

export type EdgeCountByTypeAndDs = { [edgeType: string]: { [datasource: string]: number } };

export interface BothEdgeCounts {
  incoming: EdgeCountByTypeAndDs;
  outgoing: EdgeCountByTypeAndDs;
}

const MAX_EDGE_COUNT_CACHE_ENTRIES = 500;
const edgeCountCache = new Map<string, BothEdgeCounts>();
const inFlightEdgeCountRequests = new Map<string, Promise<BothEdgeCounts>>();

function cacheKey(graph: string, encodedNodeId: string): string {
  return `${graph}::${encodedNodeId}`;
}

function cacheEdgeCounts(key: string, value: BothEdgeCounts): void {
  if (!edgeCountCache.has(key) && edgeCountCache.size >= MAX_EDGE_COUNT_CACHE_ENTRIES) {
    const oldestKey = edgeCountCache.keys().next().value;
    if (oldestKey) {
      edgeCountCache.delete(oldestKey);
    }
  }
  edgeCountCache.set(key, value);
}

export async function fetchNodeEdgeCounts(graph: string, encodedNodeId: string): Promise<BothEdgeCounts> {
  const key = cacheKey(graph, encodedNodeId);
  const cached = edgeCountCache.get(key);
  if (cached) {
    return cached;
  }

  const pending = inFlightEdgeCountRequests.get(key);
  if (pending) {
    return pending;
  }

  const request = get<BothEdgeCounts>(
    `api/v1/graphs/${graph}/nodes/${encodedNodeId}/edge_counts`
  )
    .then((result) => {
      const normalized: BothEdgeCounts = {
        incoming: result?.incoming || {},
        outgoing: result?.outgoing || {},
      };
      cacheEdgeCounts(key, normalized);
      return normalized;
    })
    .finally(() => {
      inFlightEdgeCountRequests.delete(key);
    });

  inFlightEdgeCountRequests.set(key, request);
  return request;
}

export function prefetchNodeEdgeCounts(graph: string, encodedNodeId: string): void {
  void fetchNodeEdgeCounts(graph, encodedNodeId).catch(() => {});
}
