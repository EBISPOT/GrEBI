import React, { useState } from "react";
import PropRow from './PropRow'
import GraphNode from "../../model/GraphNode";
import DatasourceSelector from "../DatasourceSelector";
import LoadingOverlay from "../LoadingOverlay";
import { PropGrid } from "./PropGrid";

export default function PropTable(params:{
    graph:string,
    node:GraphNode,
    lang:string,
    /** Loads the whole node, when it was loaded abridged; offered on the properties that were cut short */
    onLoadWhole?:()=>void,
    loadingWhole?:boolean
}) {
    let { graph, node, onLoadWhole, loadingWhole } = params

  let [datasources] =
    useState<string[]>(node.getDatasources())

  let [dsEnabled,setDsEnabled] =
    useState<string[]>(node.getDatasources().filter(ds => ds !== 'UberGraph'))

  if(!node) {
    return <LoadingOverlay message="Loading properties..." />
  }

    let props = node.getProps();

    let propkeys = Object.keys(props)

    return <div>
      <div className="pb-3">
        <DatasourceSelector datasources={datasources} dsEnabled={dsEnabled} setDsEnabled={setDsEnabled} />
      </div>
      <PropGrid>
        {propkeys.map(key =>
          <PropRow graph={graph} key={key} node={node} prop={key} values={props[key]} datasources={datasources} dsEnabled={dsEnabled}
            onLoadWhole={onLoadWhole} loadingWhole={loadingWhole} />)}
      </PropGrid>
    </div>

}
