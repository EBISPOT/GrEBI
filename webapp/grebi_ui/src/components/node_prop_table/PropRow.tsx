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
 * the tags are left out. On a node loaded abridged, a property that was cut
 * short says how many of its values are loaded and offers the whole node.
 */
export default function PropRow(params:{
    graph:string,node:GraphNode,prop:string,values:PropVal[],datasources:string[],dsEnabled:string[],
    /** Loads the whole node, for a property that was cut short */
    onLoadWhole?:()=>void,
    loadingWhole?:boolean
}) {

    let { graph, node, prop, values, dsEnabled, onLoadWhole, loadingWhole } = params

    let loaded = values.length
    // how many values the property has in all, when the node was loaded with only the first of them
    let total:number|undefined = node.getTruncated()[prop]

    values = values.filter(v => v.datasources.some(ds => dsEnabled.indexOf(ds) !== -1))

    // a property cut short keeps its row even with nothing to show: the values of the enabled datasources may be among the rest
    if(values.length === 0 && total === undefined) {
        return null
    }

    // appended in place: a hub node has 100,000 values on a property, and a copy of the group per value took a minute
    let dsSetToVals:Map<string,PropVal[]> = new Map()
    for(let v of values) {
        let dsSet = [...v.datasources].sort().join(',')
        let group = dsSetToVals.get(dsSet)
        if(!group) {
            dsSetToVals.set(dsSet, group = [])
        }
        group.push(v)
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
        {total !== undefined &&
            <div className={"text-sm italic text-neutral-dark" + (values.length > 0 ? " mt-1" : "")}>
                The first {loaded.toLocaleString()} of {total.toLocaleString()} values are loaded.{" "}
                {loadingWhole
                    ? <span>Loading the rest…</span>
                    : onLoadWhole && <button type="button" className="link-default italic" onClick={onLoadWhole}>Load all</button>}
            </div>}
    </PropGridRow>
}
