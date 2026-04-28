import { useEffect, useState } from "react";
import { get } from "../../app/api";

const LAST_GRAPH_KEY = "grebi_last_graph";

function normalizeGraph(graph?: string | null) {
  if (!graph) {
    return undefined;
  }

  const trimmed = graph.trim();
  if (!trimmed || trimmed === "undefined" || trimmed === "null") {
    return undefined;
  }

  return trimmed;
}

export default function useEffectiveGraph(graph?: string) {
  const [effectiveGraph, setEffectiveGraph] = useState<string | undefined>(() => {
    const normalizedGraph = normalizeGraph(graph);
    if (normalizedGraph) {
      return normalizedGraph;
    }

    return normalizeGraph(sessionStorage.getItem(LAST_GRAPH_KEY));
  });

  useEffect(() => {
    const normalizedGraph = normalizeGraph(graph);
    if (normalizedGraph) {
      sessionStorage.setItem(LAST_GRAPH_KEY, normalizedGraph);
      setEffectiveGraph(normalizedGraph);
      return;
    }

    const storedGraph = normalizeGraph(sessionStorage.getItem(LAST_GRAPH_KEY));
    if (storedGraph) {
      setEffectiveGraph(storedGraph);
      return;
    }

    sessionStorage.removeItem(LAST_GRAPH_KEY);

    let cancelled = false;

    get<string[]>("api/v1/graphs")
      .then((graphs) => {
        const fallbackGraph = normalizeGraph(graphs[0]);
        if (!fallbackGraph || cancelled) {
          return;
        }

        sessionStorage.setItem(LAST_GRAPH_KEY, fallbackGraph);
        setEffectiveGraph(fallbackGraph);
      })
      .catch(() => {});

    return () => {
      cancelled = true;
    };
  }, [graph]);

  return effectiveGraph;
}
