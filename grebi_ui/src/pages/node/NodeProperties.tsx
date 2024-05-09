

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

  let pageTitle = node.getName()?.value;
  let pageDesc = node.getDescription()?.value
  let refs = node.getRefs();
  let props = node.getProps();
  let datasources = node.getDatasources();
  datasources.sort((a, b) => a.localeCompare(b) + (a.startsWith("OLS.") ? 10000 : 0) + (b.startsWith("OLS.") ? -10000 : 0))

  let [dsEnabled,setDsEnabled] = useState<string[]>(datasources.filter(ds => ds !== 'UberGraph'))

  let propkeys = Object.keys(props)
  propkeys = propkeys.filter(k => k !== 'id')

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
        <Typography variant="h5">{pageTitle}</Typography>
        <Typography>{pageDesc}</Typography>
                {datasources.map((ds, i) => {
                  if (ds.startsWith("OLS.")) {
                    return <span className="mr-1">
                    <sup>{i+1}</sup>
                    <Checkbox size="small" style={{padding:0}} checked={dsEnabled.indexOf(ds) !== -1} onChangeCapture={() => toggleDsEnabled(ds)} />
                      <span
                      className="link-ontology px-2 py-0.5 rounded-md text-xs text-white uppercase"
                      title={ds.split('.')[1]}>{ds.split('.')[1]}</span></span>
                  } else {
                    return <span className="mr-1">
                    <sup>{i+1}</sup>
                    <Checkbox size="small" style={{padding:0}} checked={dsEnabled.indexOf(ds) !== -1} onChangeCapture={() => toggleDsEnabled(ds)} />
                      <span
                      className="link-datasource px-2 py-0.5 rounded-md text-xs text-white uppercase"
                      title={ds}>{ds}
                      </span>
                      </span>
                  }
                })}
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
            <Grid container spacing={1} direction="row">
              <Grid item xs={2}><b>Identifiers</b></Grid>
              <Grid item xs={10}>
                {props['id'].map(id => <span
className="bg-grey-default rounded-sm font-mono py-1 pl-2 ml-1 my-1 text-sm"
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
                {propkeys.map(key => {
                  let values = props[key].filter(v => {
                    for(let ds of v.datasources) {
                      if(dsEnabled.indexOf(ds) !== -1) {
                        return true;
                      }
                    }
                  })
                  if(values.length === 0) {
                    return <Fragment></Fragment>
                  }
                  return (
                      <Grid item xs={12}>
                        <Grid container spacing={1} direction="row">
                          <Grid item xs={2} style={{overflow:'hidden'}}>
                            <b>{key}</b>
                            </Grid>
                          <Grid item xs={10}>
                            <PropValues values={values} refs={refs} datasources={datasources} dsEnabled={dsEnabled} />
                          </Grid>
                         </Grid>
                       </Grid>
                  )
                })}
          </Grid>
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

function PropValues(props:{values:PropVal[], refs:Refs, datasources:string[], dsEnabled:string[] }) {
  let { values, refs, datasources, dsEnabled } = props;
  return (
    <div>
        {
          values.map( value => <PropValue value={value} refs={refs} datasources={datasources} dsEnabled={dsEnabled} />)
        }
      </div>
  )
}

function PropValue(props:{value:PropVal, refs:Refs, datasources:string[], dsEnabled:string[]}) {
  let { value, refs, datasources, dsEnabled } = props;
  let mapped_value = refs.get(value.value);

  // todo mapped value datasources
  if(mapped_value && mapped_value.name) {
    return (
      <span className="mr-1">
        <Link className="link-default" to={"/nodes/" + encodeNodeId(value.value)}>{mapped_value.name}</Link>
        <References valueDatasources={value.datasources} allDatasources={datasources} dsEnabled={dsEnabled} />
      </span>
    )
  } else {
    let val_to_display = typeof value.value === 'string' ? value.value : JSON.stringify(value.value)
    if(val_to_display.length > 24) {
        return <div>
            <p>{val_to_display}
            <References valueDatasources={value.datasources} allDatasources={datasources} dsEnabled={dsEnabled} /></p>
        </div>
    } else {
        return (
        <span className="mr-1">
                    <span
    className="rounded-sm font-mono py-0 pl-1 ml-1 my-1 text-sm" style={{backgroundColor:'rgb(240,240,240)'}}
    >
            {typeof value.value === 'string' ? value.value : JSON.stringify(value.value)}
            </span>
            <References valueDatasources={value.datasources} allDatasources={datasources} dsEnabled={dsEnabled} />
        </span>
        )
    }
  }
}

function References(props:{valueDatasources:string[],allDatasources:string[],dsEnabled:string[]}) {
  let { valueDatasources, allDatasources, dsEnabled } = props

  let elems:JSX.Element[] = []
  let isFirst = true;

  for(let ds of valueDatasources) {
    if(dsEnabled.indexOf(ds) === -1) {
      continue;
    }
    if(isFirst) {
      isFirst = false;
    } else {
      elems.push(<span>,</span>);
    }
    elems.push(<Tooltip style={{cursor:'pointer'}} placement="top" title={ds} slotProps={{
        popper: {
          modifiers: [
            {
              name: 'offset',
              options: {
                offset: [0, -8],
              },
            },
          ],
        },
      }}><span>{(allDatasources.indexOf(ds)+1).toString()}</span></Tooltip>)
  }
  
  return <sup>{elems}</sup>
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


