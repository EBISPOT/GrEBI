
import { Fragment, useCallback, useEffect, useRef, useState } from "react";
import { siteUrl } from "../../../app/instance";
import {
  Link,
  useNavigate,
  useParams,
  useSearchParams,
} from "react-router-dom";
import PageHead from "../../../components/PageHead";
import React from "react";
import EbiBreadcrumbsBar from "../EbiBreadcrumbsBar";
import { FormatListBulleted, CallReceived, CallMade, Share, AutoAwesome, Link as LinkIcon } from "@mui/icons-material";
import { Typography, Grid, Tabs, Tab, Box } from "@mui/material";
import SourceIdChip from "../../../components/SourceIdChip";
import { orderSourceIds } from "../../../db_links/dbLinks";
import LoadingOverlay from "../../../components/LoadingOverlay";
import EdgesInList from "../../../components/node_edge_list/EdgesList";
import GraphView from "../../../components/node_graph_view/GraphView";
import PropTable from "../../../components/node_prop_table/PropTable";
import SearchBox from "../../../components/SearchBox";
import GraphNode from "../../../model/GraphNode";
import { get, getPaginated, ApiError } from "../../../app/api";
import ErrorMessage from "../../../components/ErrorMessage";
import encodeNodeId from "../../../encodeNodeId";
import { nodePath } from "../../../app/nodePath";
import EdgesList from "../../../components/node_edge_list/EdgesList";
import NodeLinks from "../../../components/NodeLinks";
import getNodeLinksTabs, { LinksTab } from "../../../components/getNodeLinksTabs";
import NodeSimilarList from "../../../components/NodeSimilarList";
import LanguagePicker from "../../../components/LanguagePicker";
import { useEmbeddingModels } from "../../../app/useEmbeddingModels";
import { useEmbedded } from "../../../app/embedded";


export default function EbiNodePage() {
  const params = useParams();
  const [searchParams, setSearchParams] = useSearchParams();
  const graph: string = params.graph as string;
  const nodeId: string = atob(params.nodeId as string);
  const lang = searchParams.get("lang") || "en";

  let [node, setNode] = useState<GraphNode|null>(null);
  let [error, setError] = useState<any>(null);
  // The node is loaded abridged, as a hub node is over 100 MB whole; the
  // property view can ask for the whole of it, and this is the node it asked for.
  const [wholeNodeId, setWholeNodeId] = useState<string|null>(null);
  const whole = wholeNodeId === nodeId;
  const embedded = useEmbedded();
  // Inside a widget the graph view is a widget of its own (<grebi-graph>, which
  // asks for tab=graph, as the graph view's own links do): tab=graph is then
  // the graph alone, and the tabs of <grebi-node> leave the graph out.
  const graphOnly = embedded && searchParams.get("tab") === "graph";
  const tab = searchParams.get("tab") || (embedded ? "properties" : "graph");
  // the link tabs this kind of node has (see getNodeLinksTabs); the Links tab appears when there are any
  const [linksTabs, setLinksTabs] = useState<LinksTab[]>([]);

  // a change of tab or language keeps the other in the URL
  function setParam(key: string, value: string) {
    const next = new URLSearchParams(searchParams);
    next.set(key, value);
    setSearchParams(next);
  }

  // the graph view's exploration lives in ?g=, so it can be shared, bookmarked
  // and stepped back through; the current params are read through a ref so
  // the callback stays the same across renders
  const searchParamsRef = useRef(searchParams);
  searchParamsRef.current = searchParams;
  const exploration = searchParams.get("g");
  const onExplorationChange = useCallback((g: string | null, options: { replace: boolean }) => {
    const next = new URLSearchParams(searchParamsRef.current);
    if (g) {
      next.set("g", g);
    } else {
      next.delete("g");
    }
    setSearchParams(next, { replace: options.replace });
  }, [setSearchParams]);
  const navigate = useNavigate();
  const onNavigateToNode = useCallback((target: { getEncodedNodeId(): string }, options: { newTab: boolean }) => {
    const url = `/graphs/${graph}/nodes/${target.getEncodedNodeId()}?tab=graph`;
    if (options.newTab) {
      window.open(siteUrl().replace(/\/+$/, "") + url, "_blank", "noopener");
    } else {
      navigate(url);
    }
  }, [navigate, graph]);
  const { availableModels, selectedModel, setSelectedModel, hasEmbeddingModels } = useEmbeddingModels(graph);

  useEffect(() => {
    let cancelled = false;
    async function getNode() {
      try {
        let graphNode = new GraphNode(await get<any>(nodePath(graph, nodeId, { lang, whole })))
        if (!cancelled) {
          setError(null)
          setNode(graphNode)
        }
      } catch (e) {
        if (!cancelled) {
          setError(e)
        }
      }
    }
    getNode()
    return () => { cancelled = true }
  }, [nodeId, lang, whole]);

  const loadWhole = useCallback(() => setWholeNodeId(nodeId), [nodeId]);
  // the whole node was asked for and the node shown is still the abridged one
  const loadingWhole = whole && node !== null && Object.keys(node.getTruncated()).length > 0;
  const moreSourceIds = node ? (node.getTruncated()["grebi:sourceIds"] || 0) - node.getSourceIds().length : 0;

  useEffect(() => {
    setLinksTabs([]);
    if (!node) return;
    let cancelled = false;
    getNodeLinksTabs(node, graph)
      .then((tabs) => { if (!cancelled) setLinksTabs(tabs.filter((t) => t.count > 0)); })
      .catch(() => { if (!cancelled) setLinksTabs([]); });
    return () => { cancelled = true; };
  }, [node, graph]);

  const notFound = error instanceof ApiError && error.status === 404;

  return (
    <div>
      <EbiBreadcrumbsBar graph={graph} entries={[

        { url: `/graphs`, label: "Graphs" },
        { url: `/graphs/${graph}`, label: "Nodes" },

        ...(node ? (
          [{ url: `/graphs/${graph}/nodes/${encodeNodeId(nodeId)}`, label: node.getName(lang) }]
        ) : [])

      ]} />
        <PageHead>
          <meta charSet="utf-8" />
          {node && <title>{node.getName(lang)}</title>}
          {node && <meta name="description" content={node.getDescription(lang)}/>}
        </PageHead>
        { node == null && error == null && <LoadingOverlay message="Loading node..." /> }
        { error &&
      <main className="container mx-auto px-4 pt-1">
        <SearchBox graph={graph} />
        {notFound ? (
          <div role="alert" className="text-center mt-8">
            <Typography variant="h5">No node with the id <code>{nodeId}</code> in {graph}</Typography>
            <p className="mt-2 text-neutral-dark">It may be absent from this graph or have been merged into another node. Try searching for it above.</p>
          </div>
        ) : (
          <ErrorMessage what="The node" error={error} />
        )}
      </main>}
        {node !== null && !error &&
      <main className="container mx-auto px-4 pt-1">
        {!graphOnly &&
          <SearchBox graph={graph} availableModels={availableModels} selectedModel={selectedModel} onModelChange={setSelectedModel} />}
        <div className="relative text-center pb-5">
        <Typography variant="h5">{node.getName(lang)} {
          node.extractTypes().map((type) => <span key={type.shortName} style={{textTransform:'uppercase', fontVariant:'small-caps',fontWeight:'bold',fontSize:'small',verticalAlign:'middle',marginLeft:'12px'}}>{type.longName}</span>)}</Typography>
        {/* a node with labels in several languages can be read in any of them */}
        {node.getLanguages().length > 1 &&
          <div className="absolute right-0 top-0">
            <LanguagePicker languages={node.getLanguages()} lang={lang} onChangeLang={(l) => setParam("lang", l)} />
          </div>}
        </div>

        {graphOnly &&
         <GraphView graph={graph} node={node} exploration={exploration} onExplorationChange={onExplorationChange} onNavigateToNode={onNavigateToNode} />}

        {!graphOnly && <>
        <div style={{width:'90%'}} className="mx-auto">
                    <Grid container spacing={0.5} direction="row" alignItems={"center"} justifyContent={"center"} className="pb-5">
              {orderSourceIds(node.getSourceIds().map(id => id.value)).map(id => <Grid item key={id}>
                <SourceIdChip id={id} />
              </Grid>)}
              {/* a node with more identifiers than were loaded */}
              {moreSourceIds > 0 && <Grid item>
                {loadingWhole
                  ? <span className="text-sm italic text-neutral-dark">Loading the rest…</span>
                  : <button type="button" className="link-default italic text-sm" onClick={loadWhole}>+ {moreSourceIds.toLocaleString()} more</button>}
              </Grid>}
            </Grid>
            </div>

        <Typography className="text-center pb-3">{node.getDescription(lang)}</Typography>
        <Grid container spacing={1} direction="column">
            <Grid item xs={2}>
          <Tabs centered orientation="horizontal" value={tab} aria-label="basic tabs example" className="border-green justify-center" sx={{ borderBottom: 1, borderColor: 'divider' }} onChange={(e, tab) => setParam("tab", tab)}>
            {!embedded && <Tab label="Graph" icon={<Share/>} value="graph" />}
            <Tab label="Property View" icon={<FormatListBulleted/>} value="properties" />
            <Tab label="Edges In" icon={<CallReceived/>} value="edges_in" />
            <Tab label="Edges Out" icon={<CallMade/>} value="edges_out" />
            {linksTabs.length > 0 && <Tab label="Links" icon={<LinkIcon/>} value="links" />}
            {hasEmbeddingModels && <Tab label="Similar" icon={<AutoAwesome/>} value="similar" />}
          </Tabs>
          </Grid>
          <Grid item xs={10}>
        <TabPanel value={tab} index={"graph"}>
         <GraphView graph={graph} node={node} exploration={exploration} onExplorationChange={onExplorationChange} onNavigateToNode={onNavigateToNode} />
        </TabPanel>
        <TabPanel value={tab} index={"properties"}>
          <PropTable lang={lang} graph={graph} node={node} onLoadWhole={loadWhole} loadingWhole={loadingWhole} />
        </TabPanel>
        <TabPanel value={tab} index={"edges_in"}>
          <EdgesList direction="incoming" graph={graph} node={node} />
        </TabPanel>
        <TabPanel value={tab} index={"edges_out"}>
          <EdgesList direction="outgoing" graph={graph} node={node} />
        </TabPanel>
        {linksTabs.length > 0 && <TabPanel value={tab} index={"links"}>
          <NodeLinks node={node} graph={graph} tabs={linksTabs} />
        </TabPanel>}
        {hasEmbeddingModels && <TabPanel value={tab} index={"similar"}>
         <NodeSimilarList graph={graph} node={node} model={selectedModel} />
        </TabPanel>}
        </Grid>
        </Grid>
        </>}
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


