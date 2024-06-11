import { Box, Grid } from "@mui/material";
import React, { Fragment } from "react";
import GraphNode from "../../../model/GraphNode";
import PropVal from "../../../model/PropVal";
import PropRow from "./PropRow";
import PropVals from "./PropVals";
import { DatasourceTag, DatasourceTags } from "../../../components/DatasourceTag";

export default function PropRowOneDatasourceSet(params:{node:GraphNode,prop:string,values:PropVal[],datasources:string[],dsEnabled:string[]}) {

    let {node,prop,values,datasources,dsEnabled } = params

    return (
        <Fragment>
              <Grid item xs={12} style={{overflow:'hidden',padding:'8px'}} className="bg-gradient-to-r from-neutral-light to-white rounded-lg">
                <b style={{fontFamily:"'SF Mono', SFMono-Regular, ui-monospace, 'DejaVu Sans Mono', Menlo, Consolas, monospace"}}>{prop}</b>
              { datasources.length > 1 && <span>
                <DatasourceTags dss={values[0].datasources} />
                </span>}
              </Grid>
              <Grid item xs={12} style={{padding:'8px'}}>
                <div className="pl-2 mb-2">
                <PropVals node={node} prop={prop} values={values} />
                </div>
              </Grid>
           </Fragment>
      )

}

