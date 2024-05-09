
import { Fragment, useEffect, useState } from "react";
import {
  Link,
  useNavigate,
  useParams,
  useSearchParams,
} from "react-router-dom";
import { Helmet } from 'react-helmet'
import React from "react";
import LoadingOverlay from "../../components/LoadingOverlay";
import GraphNode from "../../model/GraphNode";
import { get } from "../../app/api";
import NodeProperties from "./NodeProperties";

export default function NodePage() {
  const params = useParams();
  const [searchParams, setSearchParams] = useSearchParams();
  const nodeId: string = atob(params.nodeId as string);
  const lang = searchParams.get("lang") || "en";

  let [node, setNode] = useState<GraphNode|null>(null);

  useEffect(() => {
    async function getNode() {
      setNode(new GraphNode(await get<any>(`api/v1/nodes/${nodeId}?lang=${lang}`)))
    }
    getNode()
  }, [nodeId, lang]);

  if(!node) {
    return <LoadingOverlay message="Loading node..." />
  }

  return <NodeProperties node={node} />
}
