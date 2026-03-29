import { Box, Grid } from "@mui/material";
import React, { Fragment } from "react";
import PropRow from "./PropRow";
import PropVals from "./PropVals";
import PropLabel from "./PropLabel";
import GraphNode from "../../model/GraphNode";
import PropVal from "../../model/PropVal";
import { DatasourceTags } from "../DatasourceTag";

export default function PropRowOneDatasourceSet(params:{graph:string,node:GraphNode,prop:string,values:PropVal[],datasources:string[],dsEnabled:string[]}) {

    let {graph,node,prop,values,datasources,dsEnabled } = params

    return (
        <Fragment>
              <Grid item xs={12} style={{overflow:'hidden',padding:'8px'}} className="bg-gradient-to-r from-neutral-light to-white rounded-lg">
                  <PropLabel prop={prop} refs={node.getRefs()} />
              { datasources.length > 1 && <span>
                <DatasourceTags dss={values[0].datasources} />
                </span>}
              </Grid>
              <Grid item xs={12} style={{padding:'8px'}}>
                <div className="pl-2 mb-2">
                <PropVals graph={graph} refs={node.getRefs()} values={values} />
                </div>
              </Grid>
           </Fragment>
      )

}

