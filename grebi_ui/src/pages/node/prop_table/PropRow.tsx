import { Grid } from "@mui/material";
import React, { Fragment } from "react";
import GraphNode from "../../../model/GraphNode";
import PropVal from "../../../model/PropVal";
import PropRowOneDatasourceSet from "./PropRowOneDatasourceSet";
import PropRowManyDatasourceSets from "./PropRowManyDatasourceSets";

export default function PropRow(params:{node:GraphNode,prop:string,values:PropVal[],dsEnabled:string[]}) {

    let { node, prop, values, dsEnabled } = params

    // remove any values that don't aren't asserted by at least 1 of our enabled datasources
    values = values.filter(v => {
        for(let ds of v.datasources) {
          if(dsEnabled.indexOf(ds) !== -1) {
            return true;
          }
        }
      })

      // if no values after filtering, nothing to display
      if(values.length === 0) {
        return <Fragment></Fragment>
      }

      let ds_sets = new Set()
      for(let v of values) {
        ds_sets.add([...v.datasources].sort().join(','))
      }

      if(ds_sets.size === 1) {
        return <PropRowOneDatasourceSet node={node} prop={prop} values={values} dsEnabled={dsEnabled} />
      } else {
        return <PropRowManyDatasourceSets node={node} prop={prop} values={values} dsEnabled={dsEnabled} />
      }
    }