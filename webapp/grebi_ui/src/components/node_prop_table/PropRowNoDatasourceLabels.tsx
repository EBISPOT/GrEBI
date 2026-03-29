import { Box, Grid } from "@mui/material";
import React, { Fragment } from "react";
import PropRow from "./PropRow";
import PropVals from "./PropVals";
import PropLabel from "./PropLabel";
import GraphNode from "../../model/GraphNode";
import PropVal from "../../model/PropVal";

export default function PropRowNoDatasourceLabels(params:{graph:string, node:GraphNode,prop:string,values:PropVal[]}) {

    let {graph,node,prop,values } = params

    return (
        <Fragment>
              <Grid item xs={12} style={{overflow:'hidden',padding:'8px'}} className="bg-gradient-to-r from-neutral-light to-white rounded-lg">
                  <PropLabel prop={prop} refs={node.getRefs()} />
              </Grid>
              <Grid item xs={12}>
                <PropVals graph={graph} node={node} refs={node.getRefs()} prop={prop} values={values} />
              </Grid>
           </Fragment>
      )

}

