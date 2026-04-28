import { useEffect, useState } from "react";
import { get } from "../../app/api";

const LAST_GRAPH_KEY = "grebi_last_graph";

export default function useEffectiveGraph(graph?: string) {
  const [effectiveGraph, setEffectiveGraph] = useState<string | undefined>(() => {
    return graph || sessionStorage.getItem(LAST_GRAPH_KEY) || undefined;
  });

  useEffect(() => {
    if (graph) {
      sessionStorage.setItem(LAST_GRAPH_KEY, graph);
      setEffectiveGraph(graph);
      return;
    }

    const storedGraph = sessionStorage.getItem(LAST_GRAPH_KEY);
    if (storedGraph) {
      setEffectiveGraph(storedGraph);
      return;
    }

    let cancelled = false;

    get<string[]>("api/v1/graphs")
      .then((graphs) => {
        const fallbackGraph = graphs[0];
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
