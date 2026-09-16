import React, { Fragment, useState } from "react"
import { externalLinkForDatasource, dbIconUrl } from "../db_links/dbLinks"

/**
 * A datasource as a coloured tag. With `linked` the tag links to the
 * datasource's homepage, with its icon; leave that off inside anything that
 * is itself a link or a clickable row.
 */
function DatasourceTag(props:{ds:string, linked?:boolean}) {
    let { ds, linked } = props
    const ontology = ds.startsWith("OLS.") || ds.startsWith("Ontologies.")
    const label = ontology ? ds.split('.')[1] : ds
    const className = (ontology ? "link-ontology" : "link-datasource") + " px-2 py-0.5 rounded-md text-xs text-white uppercase ml-1"
    const link = linked ? externalLinkForDatasource(ds) : null
    if(link) {
        return <a href={link.url} target="_blank" rel="noopener noreferrer" className={className}
                  title={`${label}: ${link.database}`} onClick={(e) => e.stopPropagation()}>
            {link.icon && <img src={dbIconUrl(link.icon)} alt="" width={12} height={12}
                                style={{display:'inline-block', verticalAlign:'-2px', marginRight:3}} />}
            {label}
        </a>
    }
    return <span className={className} title={label}>{label}</span>
}

let MAX_DSS = 3

function DatasourceTags(props:{dss:string[], linked?:boolean}) {
    let [ expanded, setExpanded ] = useState<boolean>(false);
    const tag = (ds:string) => <DatasourceTag key={ds} ds={ds} linked={props.linked} />
    if(props.dss.length > MAX_DSS && !expanded) {
        return <Fragment>
              {props.dss.slice(0, MAX_DSS).map(tag)}
              &nbsp;
              <span
                className="link-default italic"
                onClick={() => setExpanded(true)}
              >
                + {props.dss.length - MAX_DSS}
              </span>
            </Fragment>
    } else {
        return <Fragment>{props.dss.map(tag)}</Fragment>
    }
}

export { DatasourceTag, DatasourceTags }
