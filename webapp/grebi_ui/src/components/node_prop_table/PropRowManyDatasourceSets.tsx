import { Box, Grid } from "@mui/material";
import React, { Fragment } from "react";
import PropRow from "./PropRow";
import PropVals from "./PropVals";
import PropLabel from "./PropLabel";
import GraphNode from "../../model/GraphNode";
import PropVal from "../../model/PropVal";
import { DatasourceTags } from "../DatasourceTag";
import isSingleLineProp from "./isSingleLineProp";

export default function PropRowManyDatasourceSets(params:{graph:string,node:GraphNode,prop:string,values:PropVal[],datasources:string[],dsEnabled:string[]}) {

    let {graph,node,prop,values,dsEnabled } = params

    let dsSetToVals:Map<string,PropVal[]> = new Map()
    for(let v of values) {
      let valDsSet = [...v.datasources].sort().join(',');
      dsSetToVals.set(valDsSet, [...(dsSetToVals.get(valDsSet) || []), v])
    }
    let dsSetsSorted = Array.from(dsSetToVals.keys()).sort((a:string, b:string) => b.length - a.length)

    let allSingleOneLineValues = Array.from(dsSetToVals.values())
            .filter(vals => vals.length === 1)
            .map(vals => vals[0])
            .filter(val => isSingleLineProp(val))
            .length === Array.from(dsSetToVals.values()).length;

    if(allSingleOneLineValues) {
      return (
          <Fragment>
                <Grid item xs={12} style={{overflow:'hidden',padding:'8px'}} className="bg-gradient-to-r from-neutral-light to-white rounded-lg">
                  <PropLabel prop={prop} refs={node.getRefs()} />
                </Grid>
                {
                  dsSetsSorted.map(dsSet => {
                    let values = dsSetToVals.get(dsSet) || []
                      return <Fragment>
                      <Grid item xs={12} style={{padding:'8px'}}>
                        <DatasourceTags dss={values[0].datasources} />
                        <PropVals graph={graph} refs={node.getRefs()} values={values} />
                      </Grid>
                    </Fragment>
                  })
                }
             </Fragment>
        )
    } else {
      return (
          <Fragment>
                <Grid item xs={12} style={{overflow:'hidden',padding:'8px'}} className="bg-gradient-to-r from-neutral-light to-white rounded-lg">
                  <b style={{fontFamily:"'SF Mono', SFMono-Regular, ui-monospace, 'DejaVu Sans Mono', Menlo, Consolas, monospace"}}>{prop}</b>
                </Grid>
                {
                  dsSetsSorted.map((dsSet, i) => {
                    let values = dsSetToVals.get(dsSet) || []
                      return <Fragment>
                        <Grid item xs={12}>
                          <div className="pl-0">
                          <DatasourceTags dss={values[0].datasources} />
                          </div>
                        </Grid>
                        <Grid item xs={12}>
                        <div className={"pl-4" + (i == dsSetsSorted.length-1 ? " mb-2" : "")}>
                          <PropVals graph={graph} refs={node.getRefs()} values={values} />
                          </div>
                        </Grid>
                      </Fragment>
                  })
                }
             </Fragment>
        )
    }


}
