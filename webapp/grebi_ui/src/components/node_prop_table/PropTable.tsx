import { Grid } from "@mui/material";
import React, { Fragment, useEffect, useState } from "react";
import PropRow from './PropRow'
import GraphNode from "../../model/GraphNode";
import DatasourceSelector from "../DatasourceSelector";
import LoadingOverlay from "../LoadingOverlay";

export default function PropTable(params:{
    graph:string,
    node:GraphNode,
    lang:string
}) {
    let { graph, node, lang } = params

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

    let rows:JSX.Element[] = []

    for(let key of propkeys) {
       let elem = <PropRow graph={graph} key={key} node={node} prop={key} values={props[key]} datasources={datasources} dsEnabled={dsEnabled} /> 
       if(elem) {
          rows.push(elem)
          rows.push(<Grid item xs={12} style={{padding:'4px'}} />)
       }
    }

    return <Grid container spacing={1} direction="row">
        <Grid item className="pb-5">
      <DatasourceSelector datasources={datasources} dsEnabled={dsEnabled} setDsEnabled={setDsEnabled} />
      </Grid>
      {rows}
    </Grid>

}
