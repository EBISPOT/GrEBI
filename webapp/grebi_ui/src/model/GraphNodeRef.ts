
import PropVal from "./PropVal";
import { pickBestDisplayName, pickWorstDisplayName, readabilityScore } from "../app/util";
import encodeNodeId from "../encodeNodeId";

import hardcodedNodeTypes from "../hardcoded_node_types.json";

export default class GraphNodeRef {

    props:any

    constructor(props:any) {
        if(!props) {
            throw new Error("GraphNodeRef constructor but props are null")
        }
        this.props = props
    }

    getNodeId():string {
        return this.props['grebi:nodeId']
    }

    getEncodedNodeId():string {
        return encodeNodeId(this.props['grebi:nodeId'])
    }

    getDatasources():string[] {
        return this.props['grebi:datasources'] || []
    }

    getId():PropVal {
        if(this.props['ols:curie']){
            return PropVal.arrFrom(this.props['ols:curie'])[0]
        }
        return PropVal.from(this.props['grebi:nodeId'])
    }

    getNames():PropVal[] {
        return PropVal.arrFrom(this.props['grebi:name'] || []);
    }

    getName():string {
        let names = this.getNames();
        if (names.length > 0) {
            return names[0].value;
        } else {
            return this.getId().value;
        }
    }

    getTypes():string[] {
        return this.props['grebi:type']
    }

    getSourceIds():PropVal[] {
        return PropVal.arrFrom(this.props['grebi:sourceIds'])
    }


    extractType():{longName:string,shortName:string}|undefined {

        let types:string[] = PropVal.arrFrom(this.props['grebi:type']).map(t => t.value)

        for(let ourType of types) {
            for(let knownType of hardcodedNodeTypes) {
                if(knownType.types.indexOf(ourType) !== -1) {
                    return knownType
                }
            }
        }
    }

}
