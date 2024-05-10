import { Box, Grid } from "@mui/material";
import React, { Fragment } from "react";
import GraphNode from "../../../model/GraphNode";
import PropVal from "../../../model/PropVal";
import PropRow from "./PropRow";
import PropVals from "./PropVals";
import { DatasourceTag, DatasourceTags } from "../../../components/DatasourceTag";

export default function PropRowManyDatasourceSets(params:{node:GraphNode,prop:string,values:PropVal[],dsEnabled:string[]}) {

    let {node,prop,values,dsEnabled } = params

    let dsSetToVals:Map<string,PropVal[]> = new Map()
    for(let v of values) {
      let valDsSet = [...v.datasources].sort().join(',');
      dsSetToVals.set(valDsSet, [...(dsSetToVals.get(valDsSet) || []), v])
    }
    let dsSetsSorted = Array.from(dsSetToVals.keys()).sort((a:string, b:string) => b.length - a.length)

    let allSingleValues = Array.from(dsSetToVals.values()).filter(v => v.length > 1).length === 0;

    if(allSingleValues) {
      return (
          <Fragment>
                <Grid item xs={12} style={{overflow:'hidden'}}>
                  <b style={{fontFamily:"'SF Mono', SFMono-Regular, ui-monospace, 'DejaVu Sans Mono', Menlo, Consolas, monospace"}}>{prop}</b>
                </Grid>
                {
                  dsSetsSorted.map(dsSet => {
                    let values = dsSetToVals.get(dsSet) || []
                      return <Fragment>
                      <Grid item xs={12}>
                        <div className="pl-2">
                        <DatasourceTags dss={values[0].datasources} />
                        <PropVals node={node} prop={prop} values={values} />
                        </div>
                      </Grid>
                    </Fragment>
                  })
                }
             </Fragment>
        )
    } else {
      return (
          <Fragment>
                <Grid item xs={12} style={{overflow:'hidden'}}>
                  <b style={{fontFamily:"'SF Mono', SFMono-Regular, ui-monospace, 'DejaVu Sans Mono', Menlo, Consolas, monospace"}}>{prop}</b>
                </Grid>
                {
                  dsSetsSorted.map(dsSet => {
                    let values = dsSetToVals.get(dsSet) || []
                      return <Fragment>
                        <Grid item xs={12}>
                          <div className="pl-2">
                          <DatasourceTags dss={values[0].datasources} />
                          </div>
                        </Grid>
                        <Grid item xs={12}>
                        <div className="pl-4">
                          <PropVals node={node} prop={prop} values={values} />
                          </div>
                        </Grid>
                      </Fragment>
                  })
                }
             </Fragment>
        )
    }


}
