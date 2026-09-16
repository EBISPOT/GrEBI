import { useState } from "react";
import { externalLinkForId, orderSourceIds } from "../db_links/dbLinks";
import { DbIcon } from "./SourceIdChip";
import { Fragment } from "react/jsx-runtime";

let MAX_IDS = 3

export default function CollapsingIdList({ids}) {
    let [ expanded, setExpanded ] = useState<boolean>(false);
    // EBI-hosted ids first, then other linked ids, then the rest
    const ordered = orderSourceIds(ids.map(id => typeof id === "string" ? id : id.value));
    if(ids.length > MAX_IDS && !expanded) {
        return <div className="my-1 leading-relaxed flex flex-wrap gap-1">
              {ordered.slice(0, MAX_IDS).map(id => <Id key={id} id={id}/>)}
              <span
                className="link-default italic"
                onClick={() => setExpanded(true)}
              >
                + {ids.length - MAX_IDS}
              </span>
            </div>
    } else {
        return <div className="my-1 leading-relaxed flex flex-wrap gap-1">
            {ordered.map(id => <Id key={id} id={id}/>)}</div>
    }

}

function Id({id}: {id: string}) {
    const className = "bg-grey-default rounded-sm font-mono py-1 px-1 mr-1 text-sm"
    const link = externalLinkForId(id)
    if(link) {
        return <a className={className + " hover:underline"} href={link.url} target="_blank" rel="noopener noreferrer" title={`Open in ${link.database}`}>
            <DbIcon link={link} size={13} />{id}
        </a>
    }
    return <span className={className}>{id}</span>
}

