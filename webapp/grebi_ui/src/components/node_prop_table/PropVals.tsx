import React, { Fragment, useState } from "react";
import { Link } from "react-router-dom";
import { pickBestDisplayName } from "../../app/util";
import encodeNodeId from "../../encodeNodeId";
import GraphNode from "../../model/GraphNode";
import PropVal from "../../model/PropVal";
import ClassExpression from "../ClassExpression";
import isSingleLineProp from "./isSingleLineProp";
import Refs from "../../model/Refs";
import { SSL_OP_SSLEAY_080_CLIENT_DH_BUG } from "constants";

let MAX_VALS_ONELINE = 10
let MAX_VALS_MULTILINE = 5 

export default function PropVals(params:{ subgraph:string,refs:Refs,values:PropVal[] }) {

    let { subgraph,refs, values } = params;

    if(!refs) {
        throw new Error("refs missing")
    }

    let [ expanded, setExpanded ] = useState<boolean>(false);

    // if all values are <= 32 characters use one line and possibly monospace (if not links)
    let oneLine = values.filter(v => !isSingleLineProp(v)).length === 0;

    if(oneLine) {
        if(values.length > MAX_VALS_ONELINE && !expanded) {
            return <Fragment>
                <span>
                {
                    values.slice(0, MAX_VALS_ONELINE).map( (value,i) => <Fragment>
                        <PropValue subgraph={subgraph} refs={refs} value={value} monospace={false} separator={i > 0 ? ";" : ""} />
                        </Fragment>
                    )
                }
              </span>
              &nbsp;
              <span
                className="link-default italic"
                onClick={() => setExpanded(true)}
              >
                + {values.length - MAX_VALS_ONELINE}
              </span>
            </Fragment>
        } else {
            return <span>
                {
                    values.map( (value,i) => <Fragment>
                        <PropValue subgraph={subgraph} refs={refs} value={value} monospace={false} separator={i > 0 ? ";" : ""} />
                        </Fragment>
                    )
                }
              </span>
        }
    } else {
        if(values.length > MAX_VALS_MULTILINE && !expanded) {
            return <div style={{position:'relative'}}>
                    <div style={{position:'absolute', right:'40px', bottom:0}}>
              <span
                className="link-default italic"
                onClick={() => setExpanded(true)}
              >
                + {values.length - MAX_VALS_MULTILINE}
              </span>
              </div>
                    {
                        values.slice(0, MAX_VALS_MULTILINE).map( (value,i) => 
                            <div className={i>0?"pt-1":""}>
                            <PropValue subgraph={subgraph} refs={refs} value={value} monospace={false} separator="" />
                            </div>
                        )
                    }
                    </div>
        } else {
            return (
                <div>
                    {
                        values.map( (value,i) => 
                            <div className={i>0?"pt-1":""}>
                            <PropValue subgraph={subgraph} refs={refs} value={value} monospace={false} separator="" />
                            </div>
                        )
                    }
                    </div>
                )
        }
    }

}

function PropValue(params:{subgraph:string,refs:Refs,value:PropVal,monospace:boolean,separator:string}) {

    let { subgraph, refs, value, monospace, separator } = params;

    if(typeof value.value === 'object') {
        if(value.value["rdf:type"] !== undefined) {
            return <ClassExpression subgraph={subgraph} refs={refs} expr={value.value} />
        } else {
            return <span>{JSON.stringify(value.value)}</span>
        }
    }

    let mapped_value = refs.get(value.value);
  
    // todo mapped value datasources
    if(mapped_value) {
        var linkUrl =  "/subgraphs/" + subgraph + "/nodes/" + encodeNodeId(value.value);
      return (
        <span className="mr-0">
          {separator} <Link className="link-default" to={linkUrl}>{
            mapped_value.getName()
          }</Link>
        </span>
      )
    } else {
      let val_to_display = typeof value.value === 'string' ? value.value : JSON.stringify(value.value)
      if(!monospace) {
          return <span className="mr-0">{separator} {val_to_display}</span>
      } else {
          return (
          <span className="mr-0">
                      {separator} <span
      className="rounded-sm font-mono py-0 pl-1 ml-1 my-1 text-sm" style={{backgroundColor:'rgb(240,240,240)'}}
      >
              {value.value}
              </span>
          </span>
          )
      }
    }

}
