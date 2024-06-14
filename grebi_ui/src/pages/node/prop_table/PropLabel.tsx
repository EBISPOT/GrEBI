
import React, { Fragment } from "react";
import Refs from "../../../model/Refs";
import { pickBestDisplayName } from "../../../app/util";
import { Tooltip } from "@mui/material";

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
        displayName = pickBestDisplayName(ref.name)
        if(displayName) {
                return <b>{displayName}
                        <Tooltip
                        title={ref.id.join('; ')}
                        placement="top"
                        arrow
                    >
                    <i className="icon icon-common icon-info text-neutral-default text-sm ml-1" style={{cursor:'pointer'}} />
                    </Tooltip>
                </b>
        }
    }

    return <b style={{fontFamily:"'SF Mono', SFMono-Regular, ui-monospace, 'DejaVu Sans Mono', Menlo, Consolas, monospace"}}>
        {prop}
    </b>
}