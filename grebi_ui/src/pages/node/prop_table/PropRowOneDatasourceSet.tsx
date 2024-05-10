import { Box, Grid } from "@mui/material";
import React, { Fragment } from "react";
import GraphNode from "../../../model/GraphNode";
import PropVal from "../../../model/PropVal";
import PropRow from "./PropRow";
import PropVals from "./PropVals";
import { DatasourceTag, DatasourceTags } from "../../../components/DatasourceTag";

export default function PropRowOneDatasourceSet(params:{node:GraphNode,prop:string,values:PropVal[],dsEnabled:string[]}) {

    let {node,prop,values,dsEnabled } = params

    return (
        <Fragment>
              <Grid item xs={12} style={{overflow:'hidden'}}>
                <b style={{fontFamily:"'SF Mono', SFMono-Regular, ui-monospace, 'DejaVu Sans Mono', Menlo, Consolas, monospace"}}>{prop}</b>
                <span>
                <DatasourceTags dss={values[0].datasources} />
                </span>
              </Grid>
              <Grid item xs={12}>
                <div className="pl-2">
                <PropVals node={node} prop={prop} values={values} />
                </div>
              </Grid>
           </Fragment>
      )

}

