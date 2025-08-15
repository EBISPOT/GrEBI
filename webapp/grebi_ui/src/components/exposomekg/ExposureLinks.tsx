import { Fragment, useEffect, useState } from "react";
import GraphNode from "../../model/GraphNode";
import { getPaginated, Page } from "../../app/api";
import encodeNodeId from "../../encodeNodeId";
import { CircularProgress, Grid, Tab, Tabs, Typography } from "@mui/material";
import { asArray, copyToClipboard } from "../../app/util";
import LocalDataTable from "../datatable/LocalDataTable";
import NodeRefLink from "../node_edge_list/NodeRefLink";
import GraphEdge from "../../model/GraphEdge";
import GraphNodeRef from "../../model/GraphNodeRef";
import { DatasourceTags } from "../DatasourceTag";
import Refs from "../../model/Refs";
import PropVals from "../node_prop_table/PropVals";
import PropVal from "../../model/PropVal";
import { useSearchParams } from "react-router-dom";
import getExposureLinksTabs, { LinksTab } from "./getExposureLinksTabs";
import { OpenInNew, Share } from "@mui/icons-material";
import TabPanel from "../TabPanel";


export default function ExposureLinks({node}:{node:GraphNode}) {

  let [searchParams, setSearchParams] = useSearchParams();
  let linksTab = searchParams.get("linksTab") || "sourceids";

  let [linksTabs, setLinksTabs] = useState<LinksTab[]>([])

  useEffect(() => {
    async function getLinksTabs() {
        let tabs = await getExposureLinksTabs(node)
        setLinksTabs(tabs)
    }
    getLinksTabs()
  }, [node])

    return <Grid container spacing={1} direction="column" className="py-0">
            <Grid item xs={2} className="py-0">
    <Tabs orientation="horizontal" value={linksTab} className="bg-gray-100 border-black justify-center rounded-lg" sx={{ borderBottom: 1, borderColor: 'divider' }} onChange={(e, tab) => setSearchParams({linksTab:tab})}>
        <Tab label={
            <div>
                {/* <OpenInNew fontSize="small" style = { {verticalAlign : 'middle'} } /> */}
                Source IDs </div>
         } value={"sourceids"} className="grebi-subtab" />
        {linksTabs.map(tab => <Tab label={
            <div>
                {/* <OpenInNew fontSize="small" style = { {verticalAlign : 'middle'} } /> */}
                {tab.tabName} </div>
            } value={tab.tabId} className="grebi-subtab" />)}
    </Tabs>
    </Grid>
    <Grid item xs={10} >
    <TabPanel value={linksTab} index={"sourceids"}>
                    <Grid
                    container spacing={0.5} direction="row" alignItems={"left"} justifyContent={"left"} className="pb-5">
               {node.getSourceIds().map(id => <Grid item>
                 <div className="bg-grey-default rounded-sm font-mono pl-1" style={{fontSize:'small'}}>
                 {id.value} <button onClick={() => { copyToClipboard(id.value); }} >
                   <i className="icon icon-common icon-copy icon-spacer" />
                 </button>
                 </div>
 </Grid>
 )}
             </Grid>
    </TabPanel>
    {!linksTabs && <CircularProgress />}
    {linksTabs && linksTabs.filter(tab => tab.tabId === 'chemical_gene_interactions').length > 0 &&
    <TabPanel value={linksTab} index={"chemical_gene_interactions"}>
        <GeneExposureLinks node={node} />
    </TabPanel>
    }
             </Grid>
             </Grid>
}


let fixedCols = [
    {
        id: "grebi:datasources",
        name: "Datasources",
        selector: (edge:GraphEdge, key:string) => <DatasourceTags dss={edge['grebi:datasources']} />,
        sortable:true
    },
    {
        id: "from",
        name: "Chemical",
        selector: (edge:GraphEdge, key:string) => <NodeRefLink subgraph={process.env.REACT_APP_EXPOSOMEKG_SUBGRAPH!} nodeRef={new GraphNodeRef(edge['from'])} showTypeChip={false} />,
        sortable:true
    }
];

function DefaultSelector(row:any, key:string) {
    let vals = asArray(row[key]).map(PropVal.from);

    console.dir(vals)

    if(!row['_refs']) {
        throw new Error("No refs in row")
    }

    return <PropVals 
     subgraph={process.env.REACT_APP_EXPOSOMEKG_SUBGRAPH!} 
    refs={new Refs(row['_refs'])}
    values={vals} />
}

function GeneExposureLinks({node}:{node:GraphNode}) {

    let [affectedBy, setAffectedBy] = useState<Page<any>|null>(null)

    useEffect(() => {

        async function getAffectedBy() {
            let res = await getPaginated<any>(`api/v1/subgraphs/${
                    process.env.REACT_APP_EXPOSOMEKG_SUBGRAPH
                }/nodes/${encodeNodeId(node.getNodeId())}/incoming_edges`, {
                'grebi:type': 'biolink:chemical_gene_interaction_association'
            });
            setAffectedBy(res)
        }
         
        getAffectedBy()

    }, [node.getNodeId()])

    if(!affectedBy) {
        return <CircularProgress/>
    }

    return <LocalDataTable
                    data={affectedBy?.elements} 
                    addColumnsFromData={true}
                    columns={fixedCols}
                    maxRowHeight={"1.5em"}
                    defaultSelector={DefaultSelector}
                    hideColumns={[
                        "_refs",
                        "grebi:edgeId",
                        "grebi:subgraph",
                        "grebi:type",
                        "grebi:fromNodeId",
                        "grebi:toNodeId",
                        "grebi:fromSourceIds",
                        "grebi:name",
                        "to"
                    ]}
                    />
}

function ChemicalExposureLinks({node}:{node:GraphNode}) {

    return <div></div>
}



function ExpandableSection({title, loading, children}:{title:string, loading?:boolean|undefined, children:any}) {

    let [expanded, setExpanded] = useState<boolean>(false);

    return <div>
        <Typography variant="h6" onClick={() => setExpanded(!expanded)} style={{cursor:'pointer'}}>
            {loading ?
            <Fragment>
                <CircularProgress size="1rem" />
                &nbsp;
            </Fragment>
            :
                <Fragment>{expanded ? '-\t' : '+\t'}</Fragment>
            }
            {title}
        </Typography>
        { expanded && children }
    </div>

}