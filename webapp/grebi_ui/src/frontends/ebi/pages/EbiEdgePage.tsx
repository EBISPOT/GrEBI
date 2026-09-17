import { useState } from "react";
import { useParams } from "react-router-dom";
import { Helmet } from "react-helmet";
import { Typography } from "@mui/material";
import EbiBreadcrumbsBar from "../EbiBreadcrumbsBar";
import EdgeDetails from "../../../components/EdgeDetails";

/** One edge, by its id: the permalink the edge dialog and the edge lists point at. */
export default function EbiEdgePage() {
  const params = useParams();
  const graph: string = params.graph as string;
  const edgeId: string = atob(params.edgeId as string);
  const [edgeType, setEdgeType] = useState<string | null>(null);

  return (
    <div>
      <EbiBreadcrumbsBar graph={graph} entries={[
        { url: `/graphs`, label: "Graphs" },
        { url: `/graphs/${graph}/edges`, label: "Edges" },
        { url: `/graphs/${graph}/edges/${params.edgeId}`, label: <code>{edgeType || edgeId}</code> },
      ]} />
      <Helmet>
        <meta charSet="utf-8" />
        <title>{edgeType ? `${edgeType} edge` : "Edge"} - GrEBI</title>
      </Helmet>
      <main className="container mx-auto px-4 my-8">
        <Typography variant="h5" className="pb-4">Edge <code className="text-base">{edgeId}</code></Typography>
        <EdgeDetails graph={graph} edgeId={edgeId} onLoaded={(edge) => setEdgeType(edge?.["grebi:type"] || null)} />
      </main>
    </div>
  );
}
