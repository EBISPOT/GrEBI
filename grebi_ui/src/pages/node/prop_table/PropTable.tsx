import { Grid } from "@mui/material";
import GraphNode from "../../../model/GraphNode";
import React, { Fragment, useEffect, useState } from "react";
import PropRow from './PropRow'
import LoadingOverlay from "../../../components/LoadingOverlay";
import DatasourceSelector from "../../../components/DatasourceSelector";

export default function PropTable(params:{
    subgraph:string,
    node:GraphNode,
    lang:string
}) {
    let { subgraph, node, lang } = params

  let [datasources,setDatasources] =
    useState<string[]>(node.getDatasources())

  let [dsEnabled,setDsEnabled] =
    useState<string[]>(node.getDatasources().filter(ds => ds !== 'UberGraph')) 

  if(!node) {
    return <LoadingOverlay message="Loading properties..." />
  }
    
    let props = node.getProps();
 
    let propkeys = Object.keys(props)
    propkeys = propkeys.filter(k => k !== 'id')  

    return <Grid container spacing={1} direction="row">
      <DatasourceSelector datasources={datasources} dsEnabled={dsEnabled} setDsEnabled={setDsEnabled} />
        {propkeys.map(key => <PropRow subgraph={subgraph} key={key} node={node} prop={key} values={props[key]} datasources={datasources} dsEnabled={dsEnabled} />)}
    </Grid>

}
