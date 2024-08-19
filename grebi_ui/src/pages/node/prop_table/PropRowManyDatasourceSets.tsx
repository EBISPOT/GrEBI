import { Box, Grid } from "@mui/material";
import React, { Fragment } from "react";
import GraphNode from "../../../model/GraphNode";
import PropVal from "../../../model/PropVal";
import PropRow from "./PropRow";
import PropVals from "./PropVals";
import { DatasourceTag, DatasourceTags } from "../../../components/DatasourceTag";
import PropLabel from "./PropLabel";

export default function PropRowManyDatasourceSets(params:{subgraph:string,node:GraphNode,prop:string,values:PropVal[],datasources:string[],dsEnabled:string[]}) {

    let {subgraph,node,prop,values,dsEnabled } = params

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
                <Grid item xs={12} style={{overflow:'hidden',padding:'8px'}} className="bg-gradient-to-r from-neutral-light to-white rounded-lg">
                  <PropLabel prop={prop} refs={node.getRefs()} />
                </Grid>
                {
                  dsSetsSorted.map(dsSet => {
                    let values = dsSetToVals.get(dsSet) || []
                      return <Fragment>
                      <Grid item xs={12} style={{padding:'8px'}}>
                        <div className="pl-2">
                        <DatasourceTags dss={values[0].datasources} />
                        <PropVals subgraph={subgraph} node={node} prop={prop} values={values} />
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
                <Grid item xs={12} style={{overflow:'hidden',padding:'8px'}} className="bg-gradient-to-r from-neutral-light to-white rounded-lg">
                  <b style={{fontFamily:"'SF Mono', SFMono-Regular, ui-monospace, 'DejaVu Sans Mono', Menlo, Consolas, monospace"}}>{prop}</b>
                </Grid>
                {
                  dsSetsSorted.map((dsSet, i) => {
                    let values = dsSetToVals.get(dsSet) || []
                      return <Fragment>
                        <Grid item xs={12}>
                          <div className="pl-2">
                          <DatasourceTags dss={values[0].datasources} />
                          </div>
                        </Grid>
                        <Grid item xs={12}>
                        <div className={"pl-4" + (i == dsSetsSorted.length-1 ? " mb-2" : "")}>
                          <PropVals subgraph={subgraph} node={node} prop={prop} values={values} />
                          </div>
                        </Grid>
                      </Fragment>
                  })
                }
             </Fragment>
        )
    }


}
