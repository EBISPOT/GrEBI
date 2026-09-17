
import { Fragment, useEffect, useState } from "react";
import {
  Link,
  useNavigate,
  useParams,
  useSearchParams,
} from "react-router-dom";
import { Helmet } from 'react-helmet'
import React from "react";
import EbiBreadcrumbsBar from "../EbiBreadcrumbsBar";
import { FormatListBulleted, CallReceived, CallMade, Share, AutoAwesome } from "@mui/icons-material";
import { Typography, Grid, Tabs, Tab, Box } from "@mui/material";
import SourceIdChip from "../../../components/SourceIdChip";
import { orderSourceIds } from "../../../db_links/dbLinks";
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
import LanguagePicker from "../../../components/LanguagePicker";
import { useEmbeddingModels } from "../../../app/useEmbeddingModels";


export default function EbiNodePage() {
  const params = useParams();
  const [searchParams, setSearchParams] = useSearchParams();
  const graph: string = params.graph as string;
  const nodeId: string = atob(params.nodeId as string);
  const lang = searchParams.get("lang") || "en";

  let [node, setNode] = useState<GraphNode|null>(null);
  const tab = searchParams.get("tab") || "graph";

  // a change of tab or language keeps the other in the URL
  function setParam(key: string, value: string) {
    const next = new URLSearchParams(searchParams);
    next.set(key, value);
    setSearchParams(next);
  }
  const { availableModels, selectedModel, setSelectedModel, hasEmbeddingModels } = useEmbeddingModels(graph);

  useEffect(() => {
    async function getNode() {
      let graphNode = new GraphNode(await get<any>(`api/v1/graphs/${graph}/nodes/${encodeNodeId(nodeId)}?lang=${lang}`))
      setNode(graphNode)
    }
    getNode()
  }, [nodeId, lang]);

  return (
    <div>
      <EbiBreadcrumbsBar graph={graph} entries={[

        { url: `/graphs`, label: "Graphs" },
        { url: `/graphs/${graph}`, label: "Nodes" },

        ...(node ? (
          [{ url: `/graphs/${graph}/nodes/${encodeNodeId(nodeId)}`, label: node.getName(lang) }]
        ) : [])

      ]} />
        <Helmet>
          <meta charSet="utf-8" />
          {node && <title>{node.getName(lang)}</title>}
          {node && <meta name="description" content={node.getDescription(lang)}/>}
        </Helmet>
        { node == null && <LoadingOverlay message="Loading node..." /> }
        {node !== null &&
      <main className="container mx-auto px-4 pt-1">
        <SearchBox graph={graph} availableModels={availableModels} selectedModel={selectedModel} onModelChange={setSelectedModel} />
        <div className="relative text-center pb-5">
        <Typography variant="h5">{node.getName(lang)} {
          node.extractType()?.longName && <span style={{textTransform:'uppercase', fontVariant:'small-caps',fontWeight:'bold',fontSize:'small',verticalAlign:'middle',marginLeft:'12px'}}>{node.extractType()?.longName}</span>}</Typography>
        {/* a node with labels in several languages can be read in any of them */}
        {node.getLanguages().length > 1 &&
          <div className="absolute right-0 top-0">
            <LanguagePicker languages={node.getLanguages()} lang={lang} onChangeLang={(l) => setParam("lang", l)} />
          </div>}
        </div>

        <div style={{width:'90%'}} className="mx-auto">
                    <Grid container spacing={0.5} direction="row" alignItems={"center"} justifyContent={"center"} className="pb-5">
              {orderSourceIds(node.getSourceIds().map(id => id.value)).map(id => <Grid item key={id}>
                <SourceIdChip id={id} />
              </Grid>)}
            </Grid>
            </div>

        <Typography className="text-center pb-3">{node.getDescription(lang)}</Typography>
        <Grid container spacing={1} direction="column">
            <Grid item xs={2}>
          <Tabs centered orientation="horizontal" value={tab} aria-label="basic tabs example" className="border-green justify-center" sx={{ borderBottom: 1, borderColor: 'divider' }} onChange={(e, tab) => setParam("tab", tab)}>
            {/* <Tab label="Links" icon={<Share/>} value="links" /> */}
            <Tab label="Graph" icon={<Share/>} value="graph" />
            <Tab label="Property View" icon={<FormatListBulleted/>} value="properties" />
            <Tab label="Edges In" icon={<CallReceived/>} value="edges_in" />
            <Tab label="Edges Out" icon={<CallMade/>} value="edges_out" />
            {hasEmbeddingModels && <Tab label="Similar" icon={<AutoAwesome/>} value="similar" />}
          </Tabs>
          </Grid>
          <Grid item xs={10}>
        {/* <TabPanel value={tab} index={"links"}>
          <NodeLinks node={node} graph={graph} />
        </TabPanel> */}
        <TabPanel value={tab} index={"graph"}>
         <GraphView graph={graph} node={node} />
        </TabPanel>
        <TabPanel value={tab} index={"properties"}>
          <PropTable lang={lang} graph={graph} node={node} />
        </TabPanel>
        <TabPanel value={tab} index={"edges_in"}>
          <EdgesList direction="incoming" graph={graph} node={node} />
        </TabPanel>
        <TabPanel value={tab} index={"edges_out"}>
          <EdgesList direction="outgoing" graph={graph} node={node} />
        </TabPanel>
        {hasEmbeddingModels && <TabPanel value={tab} index={"similar"}>
         <NodeSimilarList graph={graph} node={node} model={selectedModel} />
        </TabPanel>}
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


