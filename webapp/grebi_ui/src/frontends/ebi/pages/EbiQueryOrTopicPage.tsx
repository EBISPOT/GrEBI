import { useEffect, useState } from "react";
import { useParams } from "react-router-dom";
import { Box, CircularProgress } from "@mui/material";
import QueryTopic from "../../../model/QueryTopic";
import { get } from "../../../app/api";
import EbiQueriesHomePage from "./EbiQueriesHomePage";
import EbiQueryPage from "./EbiQueryPage";

/**
 * Resolve a URL segment to a topic id: an exact id ("gwas_catalog"), or a
 * unique id prefix at an underscore boundary ("gwas" -> "gwas_catalog"), so
 * short, memorable links work without every topic needing an alias.
 */
export function resolveTopicId(topics: QueryTopic[], segment: string): string | undefined {
  const s = segment.toLowerCase();
  const exact = topics.find((t) => t.id.toLowerCase() === s);
  if (exact) return exact.id;
  const prefixed = topics.filter((t) => t.id.toLowerCase().startsWith(s + "_"));
  return prefixed.length === 1 ? prefixed[0].id : undefined;
}

let topicsPromise: Promise<QueryTopic[]> | undefined;
function loadTopics(): Promise<QueryTopic[]> {
  if (!topicsPromise) {
    topicsPromise = get<QueryTopic[]>(`api/v1/topics`).catch((e) => {
      topicsPromise = undefined;
      throw e;
    });
  }
  return topicsPromise;
}

/**
 * /graphs/:graph/queries/:queryid serves two kinds of link: a query template
 * (the query page) and a topic (the queries list with that topic ticked, e.g.
 * /queries/gwas). Topic ids and template ids don't overlap, so a segment that
 * resolves to a topic is a topic; anything else is treated as a template id.
 */
export default function EbiQueryOrTopicPage() {
  const { queryid } = useParams();
  const [topic, setTopic] = useState<string | null | undefined>(undefined);

  useEffect(() => {
    setTopic(undefined);
    if (!queryid) {
      setTopic(null);
      return;
    }
    let cancelled = false;
    loadTopics()
      .then((topics) => !cancelled && setTopic(resolveTopicId(topics, queryid) ?? null))
      .catch(() => !cancelled && setTopic(null));
    return () => {
      cancelled = true;
    };
  }, [queryid]);

  if (topic === undefined) {
    return (
      <Box sx={{ display: "flex", justifyContent: "center", py: 8 }}>
        <CircularProgress />
      </Box>
    );
  }
  return topic ? <EbiQueriesHomePage initialTopic={topic} /> : <EbiQueryPage />;
}
