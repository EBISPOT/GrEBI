import { Box, Grid } from "@mui/material";
import React, { Fragment } from "react";
import GraphNode from "../../../model/GraphNode";
import PropVal from "../../../model/PropVal";
import PropRow from "./PropRow";
import PropVals from "./PropVals";
import { DatasourceTag, DatasourceTags } from "../../../components/DatasourceTag";
import PropLabel from "./PropLabel";

export default function PropRowNoDatasourceLabels(params:{node:GraphNode,prop:string,values:PropVal[]}) {

    let {node,prop,values } = params

    return (
        <Fragment>
              <Grid item xs={12} style={{overflow:'hidden',padding:'8px'}} className="bg-gradient-to-r from-neutral-light to-white rounded-lg">
                  <PropLabel prop={prop} refs={node.getRefs()} />
              </Grid>
              <Grid item xs={12}>
                <div className="pl-2 mb-2">
                <PropVals node={node} prop={prop} values={values} />
                </div>
              </Grid>
           </Fragment>
      )

}

