import React, { Fragment, useState } from "react"

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

let MAX_DSS = 3

function DatasourceTags(props:{dss:string[]}) {
    let [ expanded, setExpanded ] = useState<boolean>(false);
    if(props.dss.length > MAX_DSS && !expanded) {
        return <Fragment>
              {props.dss.slice(0, MAX_DSS).map(ds => <DatasourceTag ds={ds}/>)}
              &nbsp;
              <span
                className="link-default italic"
                onClick={() => setExpanded(true)}
              >
                + {props.dss.length - MAX_DSS}
              </span>
            </Fragment>
    } else {
        return <Fragment>{props.dss.map(ds => <DatasourceTag ds={ds}/>)}</Fragment>
    }
}

export { DatasourceTag, DatasourceTags }
