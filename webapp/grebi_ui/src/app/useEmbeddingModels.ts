import { useEffect, useState } from "react";
import { useSearchParams } from "react-router-dom";
import { get } from "./api";

export interface EmbeddingModel {
  model: string;
  can_embed: boolean;
}

export function useEmbeddingModels(graph: string) {
  const [searchParams] = useSearchParams();
  const [availableModels, setAvailableModels] = useState<EmbeddingModel[]>([]);
  const [selectedModel, setSelectedModel] = useState<string>(searchParams.get("model") || "");

  useEffect(() => {
    async function fetchModels() {
      try {
        const models = await get<EmbeddingModel[]>(`api/v1/graphs/${graph}/embedding_models`);
        setAvailableModels(models || []);
        if (!searchParams.get("model") && models && models.length > 0) {
          const embeddable = models.filter(m => m.can_embed).sort((a, b) => a.model.localeCompare(b.model));
          if (embeddable.length > 0) {
            setSelectedModel(embeddable[0].model);
          } else {
            // No embeddable models, but precomputed embeddings exist — default to first available
            const sorted = [...models].sort((a, b) => a.model.localeCompare(b.model));
            setSelectedModel(sorted[0].model);
          }
        } else if (!searchParams.get("model")) {
          setSelectedModel("lexical");
        }
      } catch (e) {
        setAvailableModels([]);
        if (!searchParams.get("model")) {
          setSelectedModel("lexical");
        }
      }
    }
    fetchModels();
  }, [graph]);

  const hasEmbeddingModels = availableModels.length > 0;

  return { availableModels, selectedModel, setSelectedModel, hasEmbeddingModels };
}
