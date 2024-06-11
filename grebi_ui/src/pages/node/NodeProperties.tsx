

import { AccountTree, Share, Visibility, VisibilityOff } from "@mui/icons-material";
import { Fragment, useEffect, useState } from "react";
import {
  Link,
  useNavigate,
  useParams,
  useSearchParams,
} from "react-router-dom";
import { Helmet } from 'react-helmet'
import React from "react";
import Header from "../../components/Header";
import LoadingOverlay from "../../components/LoadingOverlay";
import GraphNode from "../../model/GraphNode";
import { get } from "../../app/api";
import LanguagePicker from "../../components/LanguagePicker";
import ApiLinks from "../../components/ApiLinks";
import { Box, Checkbox, Grid, Tab, Tabs, Tooltip, Typography } from "@mui/material";
import Refs from "../../model/Refs";
import encodeNodeId from "../../encodeNodeId";
import PropVal from "../../model/PropVal";
import { DatasourceTags } from '../../components/DatasourceTag'
import { copyToClipboard } from "../../app/util";
import SearchBox from "../../components/SearchBox";
import PropTable from "./prop_table/PropTable";

export default function NodeProperties(params:{node:GraphNode}) {

    let {node} = params
  let [tab, setTab] = useState("properties");

  let toggleDsEnabled=(ds:string)=>{
    if(dsEnabled.indexOf(ds) !== -1) {
      setDsEnabled(dsEnabled.filter(ds2=>ds2!==ds))
    } else {
      setDsEnabled([...dsEnabled,ds])
    }
  }

  let pageTitle = node.getName();
  let pageDesc = node.getDescription()?.value
  let props = node.getProps();
  let refs = node.getRefs();
  let datasources = node.getDatasources();
  datasources.sort((a, b) => a.localeCompare(b) + (a.startsWith("OLS.") ? 10000 : 0) + (b.startsWith("OLS.") ? -10000 : 0))

  let [dsEnabled,setDsEnabled] = useState<string[]>(datasources.filter(ds => ds !== 'UberGraph'))

  return (
    <div>
      <Header section="explore" />
        <Helmet>
          <meta charSet="utf-8" />
          {pageTitle && <title>{pageTitle}</title>}
          {pageDesc && <meta name="description" content={pageDesc}/>}
        </Helmet>
      <main className="container mx-auto px-4 pt-1">
        <SearchBox/>
        <div className="text-center">
        <Typography variant="h5">{pageTitle} {
          node.extractType()?.long && <span style={{textTransform:'uppercase', fontVariant:'small-caps',fontWeight:'bold',fontSize:'small',verticalAlign:'middle',marginLeft:'12px'}}>{node.extractType()?.long}</span>}</Typography>
                      <Grid item xs={10} className="pt-2">
                {props['id'].map(id => <span
className="bg-grey-default rounded-sm font-mono py-1 pl-2 ml-1 my-1 mb-2 text-sm"
>{id.value}
<button
                    onClick={() => {
                      copyToClipboard(id.value);
                    }}
                  >
                    &nbsp;
                    <i className="icon icon-common icon-copy icon-spacer" />
                  </button>
</span>)}
              </Grid>
        <Typography>{pageDesc}</Typography>
        <div className="pt-2">
                {datasources.map((ds, i) => {
                  if (ds.startsWith("OLS.")) {
                    return <span className="mr-1">
                    { datasources.length > 1 && <Checkbox size="small" style={{padding:0}} checked={dsEnabled.indexOf(ds) !== -1} onChangeCapture={() => toggleDsEnabled(ds)} />}
                      <span
                      className="link-ontology px-2 py-0.5 rounded-md text-xs text-white uppercase"
                      title={ds.split('.')[1]}>{ds.split('.')[1]}</span></span>
                  } else {
                    return <span className="mr-1">
                    { datasources.length > 1 && <Checkbox size="small" style={{padding:0}} checked={dsEnabled.indexOf(ds) !== -1} onChangeCapture={() => toggleDsEnabled(ds)} />}
                      <span
                      className="link-datasource px-2 py-0.5 rounded-md text-xs text-white uppercase"
                      title={ds}>{ds}
                      </span>
                      </span>
                  }
                })}</div>
        </div>
        <Grid container spacing={1} direction="row">
            <Grid item xs={2}>
          <Tabs orientation="vertical" variant="scrollable" value={tab} aria-label="basic tabs example" sx={{ borderRight: 1, borderColor: 'divider' }}>
            <Tab label="Properties" value="properties" />
            <Tab label="Edges" value="edges" />
            <Tab label="Mappings" value="mappings" />
          </Tabs>
          </Grid>
          <Grid item xs={10}>
        <TabPanel value={tab} index={"properties"}>
          <PropTable node={node} datasources={datasources} dsEnabled={dsEnabled} />
        </TabPanel>
        <TabPanel value={tab} index={"edges"}>
          Item Two
        </TabPanel>
        <TabPanel value={tab} index={"mappings"}>
          Item Three
        </TabPanel>
        </Grid>
        </Grid>
      </main>

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
        <Box sx={{ p: 3 }}>
          <Typography>{children}</Typography>
        </Box>
      )}
    </div>
  );
}


