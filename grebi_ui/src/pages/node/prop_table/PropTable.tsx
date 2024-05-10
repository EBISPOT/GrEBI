import { Grid } from "@mui/material";
import GraphNode from "../../../model/GraphNode";
import React, { Fragment } from "react";
import PropRow from './PropRow'

export default function PropTable(params:{
    node:GraphNode,
    dsEnabled:string[]
}) {
    let { node, dsEnabled } = params
    
    let props = node.getProps();
 
    let propkeys = Object.keys(props)
    propkeys = propkeys.filter(k => k !== 'id')  

    return <Grid container spacing={1} direction="row">
        {propkeys.map(key => <PropRow key={key} node={node} prop={key} values={props[key]} dsEnabled={dsEnabled} />)}
    </Grid>

}
