
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
        if(this.props['grebi:curie']){
            return PropVal.arrFrom(this.props['grebi:curie'])[0]
        }
        if(this.props['ols:curie']){
            return PropVal.arrFrom(this.props['ols:curie'])[0]
        }
        return PropVal.from(this.props['grebi:nodeId'])
    }

    getNames():PropVal[] {
        return PropVal.arrFrom(this.props['grebi:name'] || []);
    }

    /**
     * The languages the node has values in, "en" first; empty when nothing on
     * the node is translated.
     */
    getLanguages():string[] {
        return this.props['grebi:languages'] || []
    }

    /**
     * The name in the language asked for; else the name in the graph's own
     * language, which is an untagged value; else whatever name comes first;
     * else the id.
     */
    getName(lang?:string):string {
        return GraphNodeRef.pickValue(this.getNames(), lang) ?? this.getId().value;
    }

    static pickValue(values:PropVal[], lang?:string):string|undefined {
        if (values.length === 0) {
            return undefined;
        }
        let wanted = lang ? values.find(v => v.language() === lang.toLowerCase()) : undefined;
        return (wanted || values.find(v => !v.language()) || values[0]).value;
    }

    getTypes():string[] {
        return this.props['grebi:type']
    }

    getSourceIds():PropVal[] {
        return PropVal.arrFrom(this.props['grebi:sourceIds'])
    }


    extractType():{longName:string,shortName:string}|undefined {

        let types:string[] = PropVal.arrFrom(this.props['grebi:type']).map(t => t.value)

        let bestIndex = -1
        let bestType:any = undefined

        for(let ourType of types) {
            for(let i = 0; i < hardcodedNodeTypes.length; i++) {
                let knownType = hardcodedNodeTypes[i]
                if(knownType.types.indexOf(ourType) !== -1) {
                    if(bestIndex === -1 || i < bestIndex) {
                        bestIndex = i
                        bestType = knownType
                    }
                }
            }
        }

        return bestType
    }

}
