
import { Fragment, useEffect, useState } from "react";
import {
  Link,
  useNavigate,
  useParams,
  useSearchParams,
} from "react-router-dom";
import { Helmet } from 'react-helmet'
import React from "react";
import EbiHeader from "../EbiHeader";
import { FormatListBulleted, CallReceived, CallMade, Share, AutoAwesome } from "@mui/icons-material";
import { Typography, Grid, Tabs, Tab, Box } from "@mui/material";
import { copyToClipboard } from "../../../app/util";
import LoadingOverlay from "../../../components/LoadingOverlay";
import EdgesInList from "../../../components/node_edge_list/EdgesList";
import GraphView from "../../../components/node_graph_view/GraphView";
import PropTable from "../../../components/node_prop_table/PropTable";
import SearchBox from "../../../components/SearchBox";
import GraphNode from "../../../model/GraphNode";
import { get, getPaginated } from "../../../app/api";
import encodeNodeId from "../../../encodeNodeId";
import EdgesList from "../../../components/node_edge_list/EdgesList";
import NodeLinks from "../../../components/NodeLinks";
import NodeSimilarList from "../../../components/NodeSimilarList";


export default function EbiNodePage() {
  const params = useParams();
  const [searchParams, setSearchParams] = useSearchParams();
  const subgraph: string = params.subgraph as string;
  const nodeId: string = atob(params.nodeId as string);
  const lang = searchParams.get("lang") || "en";

  let [node, setNode] = useState<GraphNode|null>(null);
  const tab = searchParams.get("tab") || "graph";

  useEffect(() => {
    async function getNode() {
      let graphNode = new GraphNode(await get<any>(`api/v1/subgraphs/${subgraph}/nodes/${encodeNodeId(nodeId)}?lang=${lang}`))
      setNode(graphNode)
    }
    getNode()
  }, [nodeId, lang]);

  return (
    <div>
      <EbiHeader section="explore" subgraph={subgraph} showBreadcrumbsBar={true} breadcrumbs={[

        { url: `/subgraphs/${subgraph}`, label: "Nodes" },

        ...(node ? (
          [{ url: `/subgraphs/${subgraph}/nodes/${encodeNodeId(nodeId)}`, label: node.getName() }]
        ) : [])

      ]} />
        <Helmet>
          <meta charSet="utf-8" />
          {node && <title>{node.getName()}</title>}
          {node && <meta name="description" content={node.getDescription()}/>}
        </Helmet>
        { node == null && <LoadingOverlay message="Loading node..." /> }
        {node !== null &&
      <main className="container mx-auto px-4 pt-1">
        <SearchBox subgraph={subgraph} />
        <div className="text-center pb-5">
        <Typography variant="h5">{node.getName()} {
          node.extractType()?.longName && <span style={{textTransform:'uppercase', fontVariant:'small-caps',fontWeight:'bold',fontSize:'small',verticalAlign:'middle',marginLeft:'12px'}}>{node.extractType()?.longName}</span>}</Typography>
        </div>

        <div style={{width:'90%'}} className="mx-auto">
                    <Grid container spacing={0.5} direction="row" alignItems={"center"} justifyContent={"center"} className="pb-5">
              {node.getSourceIds().map(id => <Grid item>
                <div className="bg-grey-default rounded-sm font-mono pl-1" style={{fontSize:'small'}}>
                {id.value} <button onClick={() => { copyToClipboard(id.value); }} >
                  <i className="icon icon-common icon-copy icon-spacer" />
                </button>
                </div>
</Grid>
)}
            </Grid>
            </div>

        <Typography className="text-center pb-3">{node.getDescription()}</Typography>
        <Grid container spacing={1} direction="column">
            <Grid item xs={2}>
          <Tabs centered orientation="horizontal" value={tab} aria-label="basic tabs example" className="border-green justify-center" sx={{ borderBottom: 1, borderColor: 'divider' }} onChange={(e, tab) => setSearchParams({tab})}>
            {/* <Tab label="Links" icon={<Share/>} value="links" /> */}
            <Tab label="Graph" icon={<Share/>} value="graph" />
            <Tab label="Property View" icon={<FormatListBulleted/>} value="properties" />
            <Tab label="Edges In" icon={<CallReceived/>} value="edges_in" />
            <Tab label="Edges Out" icon={<CallMade/>} value="edges_out" />
            <Tab label="Similar" icon={<AutoAwesome/>} value="similar" />
          </Tabs>
          </Grid>
          <Grid item xs={10}>
        {/* <TabPanel value={tab} index={"links"}>
          <NodeLinks node={node} subgraph={subgraph} />
        </TabPanel> */}
        <TabPanel value={tab} index={"graph"}>
         <GraphView subgraph={subgraph} node={node} />
        </TabPanel>
        <TabPanel value={tab} index={"properties"}>
          <PropTable lang={lang} subgraph={subgraph} node={node} />
        </TabPanel>
        <TabPanel value={tab} index={"edges_in"}>
          <EdgesList direction="incoming" subgraph={subgraph} node={node} />
        </TabPanel>
        <TabPanel value={tab} index={"edges_out"}>
          <EdgesList direction="outgoing" subgraph={subgraph} node={node} />
        </TabPanel>
        <TabPanel value={tab} index={"similar"}>
         <NodeSimilarList subgraph={subgraph} node={node} />
        </TabPanel>
        </Grid>
        </Grid>
      </main>}

    </div>
  );
}

interface TabPanelProps {
  children?: React.ReactNode;
  index: string;
  value: string;
}

function TabPanel(props: TabPanelProps) {
  const { children, value, index, ...other } = props;

  return (
    <div
      role="tabpanel"
      hidden={value !== index}
      id={`vertical-tabpanel-${index}`}
      aria-labelledby={`vertical-tab-${index}`}
      {...other}
    >
      {value === index && (
        <div className="pl-2">
          {children}
        </div>
      )}
    </div>
  );
}


