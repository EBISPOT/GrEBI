
import { Fragment, useEffect, useState } from "react";
import {
  Link,
  useNavigate,
  useParams,
  useSearchParams,
} from "react-router-dom";
import { Helmet } from 'react-helmet'
import React from "react";
import EkgHeader from "../EkgHeader";
import { FormatListBulleted, CallReceived, CallMade, Share, Masks, Summarize, UnfoldMoreDouble, LibraryBooks } from "@mui/icons-material";
import { Typography, Grid, Tabs, Tab, Box } from "@mui/material";
import { copyToClipboard } from "../../../app/util";
import LoadingOverlay from "../../../components/LoadingOverlay";
import EdgesList from "../../../components/node_edge_list/EdgesList";
import GraphView from "../../../components/node_graph_view/GraphView";
import PropTable from "../../../components/node_prop_table/PropTable";
import SearchBox from "../../../components/SearchBox";
import GraphNode from "../../../model/GraphNode";
import { get, getPaginated } from "../../../app/api";
import encodeNodeId from "../../../encodeNodeId";
import ExposureLinks from "../../../components/exposomekg/ExposureLinks";
import TabPanel from "../../../components/TabPanel";


export default function EkgNodePage() {
  const params = useParams();
  const [searchParams, setSearchParams] = useSearchParams();
  const nodeId: string = atob(params.nodeId as string);
  const lang = searchParams.get("lang") || "en";

  let [node, setNode] = useState<GraphNode|null>(null);
  const tab = searchParams.get("tab") || "links";

  useEffect(() => {
    async function getNode() {
      let graphNode = new GraphNode(await get<any>(`api/v1/subgraphs/${process.env.REACT_APP_EXPOSOMEKG_SUBGRAPH}/nodes/${encodeNodeId(nodeId)}?lang=${lang}`))
      setNode(graphNode)
    }
    getNode()
  }, [nodeId, lang]);

  if(!node) {
    return <LoadingOverlay message="Loading node..." />
  }

  let pageTitle = node.getName();
  let pageDesc = node.getDescription()
  let props = node.getProps();
  let refs = node.getRefs();

  return (
    <div>
      <EkgHeader section="explore" />
        <Helmet>
          <meta charSet="utf-8" />
          {pageTitle && <title>{pageTitle}</title>}
          {pageDesc && <meta name="description" content={pageDesc}/>}
        </Helmet>
      <main className="container mx-auto px-4 pt-1">
        <SearchBox subgraph={process.env.REACT_APP_EXPOSOMEKG_SUBGRAPH} />
        <div className="text-center pb-5">
        <Typography variant="h5">{pageTitle} {
          node.extractType()?.longName && <span style={{textTransform:'uppercase', fontVariant:'small-caps',fontWeight:'bold',fontSize:'small',verticalAlign:'middle',marginLeft:'12px'}}>{node.extractType()?.longName}</span>}</Typography>
        </div>
        <Typography className="text-center pb-3">{pageDesc}</Typography>
        <Grid container spacing={1} direction="column">
            <Grid item xs={2}>
          <Tabs orientation="horizontal" value={tab} aria-label="basic tabs example" className="border-green justify-center" sx={{ borderBottom: 1, borderColor: 'divider' }} onChange={(e, tab) => setSearchParams({tab})}>
            <Tab label="Links" icon={<Share/>} value="links" />
            <Tab label="Property View" icon={<FormatListBulleted/>} value="properties" />
            <Tab label="Edges In" icon={<CallReceived/>} value="edges_in" />
            <Tab label="Edges Out" icon={<CallMade/>} value="edges_out" />
            <Tab label="Graph" icon={<Share/>} value="graph" />
          </Tabs>
          </Grid>
          <Grid item xs={10}>
        <TabPanel value={tab} index={"links"}>
          <ExposureLinks node={node} />
        </TabPanel>
        <TabPanel value={tab} index={"properties"}>
          <PropTable lang={lang} subgraph={process.env.REACT_APP_EXPOSOMEKG_SUBGRAPH!} node={node} />
        </TabPanel>
        <TabPanel value={tab} index={"edges_in"}>
          <EdgesList direction="incoming" subgraph={process.env.REACT_APP_EXPOSOMEKG_SUBGRAPH!} node={node} />
        </TabPanel>
        <TabPanel value={tab} index={"edges_out"}>
          <EdgesList direction="outgoing" subgraph={process.env.REACT_APP_EXPOSOMEKG_SUBGRAPH!} node={node} />
        </TabPanel>
        <TabPanel value={tab} index={"graph"}>
         <GraphView subgraph={process.env.REACT_APP_EXPOSOMEKG_SUBGRAPH!} node={node} />
        </TabPanel>
        </Grid>
        </Grid>
      </main>

    </div>
  );
}
