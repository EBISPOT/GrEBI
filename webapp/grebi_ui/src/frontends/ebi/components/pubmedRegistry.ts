import pubmedCache from "../../../../../../docs/pubmed_cache.json";

// Module-level registry: maps PMID → assigned reference number.
// Shared between PubmedCitation and PubmedReferences.
const pmidToNum = new Map<string, number>();
let counter = 0;
let generation = 0;
const listeners = new Set<() => void>();

function notify() {
  listeners.forEach((fn) => fn());
}

export function subscribe(listener: () => void): () => void {
  listeners.add(listener);
  return () => { listeners.delete(listener); };
}

export function getSnapshot(): number {
  return generation;
}

export function resetPubmedRefs() {
  pmidToNum.clear();
  counter = 0;
  generation++;
  notify();
}

export function registerPmid(pmid: string): number {
  if (!pmidToNum.has(pmid)) {
    pmidToNum.set(pmid, ++counter);
    generation++;
    notify();
  }
  return pmidToNum.get(pmid)!;
}

export function getRegisteredRefs(): Array<{
  pmid: string;
  num: number;
  entry: any;
}> {
  const cache = pubmedCache as Record<string, any>;
  return [...pmidToNum.entries()]
    .sort((a, b) => a[1] - b[1])
    .map(([pmid, num]) => ({ pmid, num, entry: cache[pmid] || null }));
}
