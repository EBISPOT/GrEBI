import React from "react";
import PropVals from "./PropVals";
import PropLabel from "./PropLabel";
import GraphNode from "../../model/GraphNode";
import PropVal from "../../model/PropVal";
import { DatasourceTags } from "../DatasourceTag";
import { PropGridRow } from "./PropGrid";

/**
 * One property of a node in the property table: its label, and its values
 * grouped by the datasources that assert them, each group tagged with those
 * datasources (largest group of datasources first). Only the values asserted
 * by an enabled datasource are shown, and with a single datasource enabled
 * the tags are left out.
 */
export default function PropRow(params:{graph:string,node:GraphNode,prop:string,values:PropVal[],datasources:string[],dsEnabled:string[]}) {

    let { graph, node, prop, values, dsEnabled } = params

    values = values.filter(v => v.datasources.some(ds => dsEnabled.indexOf(ds) !== -1))

    if(values.length === 0) {
        return null
    }

    let dsSetToVals:Map<string,PropVal[]> = new Map()
    for(let v of values) {
        let dsSet = [...v.datasources].sort().join(',')
        dsSetToVals.set(dsSet, [...(dsSetToVals.get(dsSet) || []), v])
    }
    let dsSetsSorted = Array.from(dsSetToVals.keys()).sort((a, b) => b.length - a.length)

    let showTags = dsEnabled.length > 1

    return <PropGridRow label={<PropLabel prop={prop} refs={node.getRefs()} />}>
        {dsSetsSorted.map((dsSet, i) => {
            let vals = dsSetToVals.get(dsSet) || []
            // the tags lead the values: on the same line when the values fit
            // on one, else on a line of their own above them
            return <div key={dsSet} className={i > 0 ? "mt-1" : ""}>
                {showTags && <span className="mr-1"><DatasourceTags dss={vals[0].datasources} linked /></span>}
                <PropVals graph={graph} refs={node.getRefs()} values={vals} />
            </div>
        })}
    </PropGridRow>
}
