import React, { Fragment } from "react"

function DatasourceTag(props:{ds:string}) {
    let { ds } = props

    if(ds.startsWith("OLS.")) {
     return <span
              className="link-ontology px-2 py-0.5 rounded-md text-xs text-white uppercase ml-1"
              title={ds.split('.')[1]}>{ds.split('.')[1]}</span>
    } else {
     return <span
              className="link-datasource px-2 py-0.5 rounded-md text-xs text-white uppercase ml-1"
              title={ds}>{ds}</span>
    }
}

function DatasourceTags(props:{dss:string[]}) {
    return <Fragment>{props.dss.map(ds => <DatasourceTag ds={ds}/>)}</Fragment>
}

export { DatasourceTag, DatasourceTags }
