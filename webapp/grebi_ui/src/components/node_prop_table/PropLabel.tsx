
import React, { Fragment } from "react";
import { Tooltip } from "@mui/material";
import { pickBestDisplayName } from "../../app/util";
import Refs from "../../model/Refs";

/**
 * An identifier with places to break it after its separators, so that a long
 * one wraps there (biolink:chemical_to_ / disease_or_ ...) and not mid-word.
 */
function breakable(id:string) {
    return id.split(/([:_/#.])/).map((part, i) => i % 2 === 1 ? <Fragment key={i}>{part}<wbr /></Fragment> : part)
}

export default function PropLabel(params:{prop:string,refs:Refs}) {

    let { prop, refs } = params

    let ref = refs.get(prop)

    let displayName:string|undefined = ({
        'grebi:name': 'Name',
        'grebi:synonym': 'Synonym',
        'grebi:description': 'Description',
        'grebi:type': 'Type',
    })[prop]

    if(displayName) {
        return <b>{displayName}</b>
    }

    if(ref) {
        displayName = pickBestDisplayName(ref.getNames().map(n=>n.value as string))
        if(displayName) {
                return <b>{displayName.indexOf(' ') === -1 ? breakable(displayName) : displayName}
                        <Tooltip
                        title={ref.getSourceIds().map(sid=>sid.value).join('; ')}
                        placement="top"
                        arrow
                    >
                    <i className="icon icon-common icon-info text-neutral-default text-sm ml-1" style={{cursor:'pointer'}} />
                    </Tooltip>
                </b>
        }
    }

    return <b style={{fontFamily:"'SF Mono', SFMono-Regular, ui-monospace, 'DejaVu Sans Mono', Menlo, Consolas, monospace"}}>
        {breakable(prop)}
    </b>
}