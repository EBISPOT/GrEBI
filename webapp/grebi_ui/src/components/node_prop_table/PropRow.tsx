import { Grid } from "@mui/material";
import React, { Fragment } from "react";
import PropRowOneDatasourceSet from "./PropRowOneDatasourceSet";
import PropRowManyDatasourceSets from "./PropRowManyDatasourceSets";
import PropRowNoDatasourceLabels from "./PropRowNoDatasourceLabels";
import GraphNode from "../../model/GraphNode";
import PropVal from "../../model/PropVal";

export default function PropRow(params:{graph:string,node:GraphNode,prop:string,values:PropVal[],datasources:string[],dsEnabled:string[]}) {

    let { graph, node, prop, values, datasources, dsEnabled } = params

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
        return null
      }

      // if only 1 datasource is enabled, no need to display datasource labels anywhere
      if(dsEnabled.length === 1) {
        return <PropRowNoDatasourceLabels graph={graph} node={node} prop={prop} values={values} />
      }

      let ds_sets = new Set()
      for(let v of values) {
        ds_sets.add([...v.datasources].sort().join(','))
      }

      if(ds_sets.size === 1) {
        // [prop name] [datasources]
        //   [value]
        //
        return <PropRowOneDatasourceSet graph={graph} node={node} prop={prop} values={values} datasources={datasources} dsEnabled={dsEnabled} />
      } else {
        // [prop name]
        //    [datasources1]
        //      [value1]
        //    [datasources2]
        //      [value2]
        //
        return <PropRowManyDatasourceSets graph={graph} node={node} prop={prop} values={values} datasources={datasources} dsEnabled={dsEnabled} />
      }
    }