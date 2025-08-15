import { useState } from "react";
import { Fragment } from "react/jsx-runtime";

let MAX_IDS = 3

export default function CollapsingIdList({ids}) {
    let [ expanded, setExpanded ] = useState<boolean>(false);
    if(ids.length > MAX_IDS && !expanded) {
        return <div className="my-1 leading-relaxed flex flex-wrap gap-1">
              {ids.slice(0, MAX_IDS).map(id => <Id id={id}/>)}
              <span
                className="link-default italic"
                onClick={() => setExpanded(true)}
              >
                + {ids.length - MAX_IDS}
              </span>
            </div>
    } else {
        return <div className="my-1 leading-relaxed flex flex-wrap gap-1">
            {ids.map(id => <Id id={id}/>)}</div>
    }

}

function Id({id}) {

    return <span
className="bg-grey-default rounded-sm font-mono py-1 px-1 mr-1 text-sm"
>{id.value}</span>

}

