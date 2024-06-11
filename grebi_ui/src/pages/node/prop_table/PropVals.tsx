import React, { Fragment } from "react";
import { Link } from "react-router-dom";
import encodeNodeId from "../../../encodeNodeId";
import GraphNode from "../../../model/GraphNode";
import PropVal from "../../../model/PropVal";
import Refs from "../../../model/Refs";
import ClassExpression from "../../../components/ClassExpression";
import { pickBestDisplayName } from "../../../app/util";

export default function PropVals(params:{ node:GraphNode,prop:string,values:PropVal[] }) {

    let { node, prop, values } = params;

    // if all values are <= 32 characters use one line and possibly monospace (if not links)
    let oneLine = values.filter(v => v.value.toString().length > 48).length == 0;

    if(oneLine) {
        return (
        <span>
            {
                values.map( (value,i) => <Fragment>
                    <PropValue node={node} prop={prop} value={value} monospace={false} separator={i > 0 ? ";" : ""} />
                    </Fragment>
                )
            }
            </span>
        )
    } else {
        return (
            <div>
                {
                    values.map( (value,i) => <p>
                        <PropValue node={node} prop={prop} value={value} monospace={false} separator="" />
                        </p>
                    )
                }
                </div>
            )
    }

}

function PropValue(params:{node:GraphNode,prop:string,value:PropVal,monospace:boolean,separator:string}) {

    let { node, prop, value, monospace, separator } = params;

    if(typeof value.value === 'object') {
        if(value.value["rdf:type"] !== undefined) {
            return <ClassExpression node={node} expr={value.value} />
        } else {
            return <span>{JSON.stringify(value.value)}</span>
        }
    }

    let mapped_value = node.getRefs().get(value.value);
  
    // todo mapped value datasources
    if(mapped_value && mapped_value.name) {
      return (
        <span className="mr-0">
          {separator} <Link className="link-default" to={"/nodes/" + encodeNodeId(value.value)}>{pickBestDisplayName(mapped_value.name)}</Link>
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
