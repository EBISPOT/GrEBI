
import encodeNodeId from "../encodeNodeId";
import GraphNodeRef from "./GraphNodeRef";
import PropVal from "./PropVal";
import Refs from "./Refs";

export default class GraphNode extends GraphNodeRef {

    props:any

    constructor(props:any) {
        super(props)
    }

    getSynonyms():PropVal[] {
        return PropVal.arrFrom(this.props['grebi:synonym'] || []);
    }

    getDescriptions():PropVal[] {
        return PropVal.arrFrom(this.props['grebi:description'])
    }

    getDescription():string|undefined {
        return PropVal.arrFrom(this.getDescriptions())[0]?.value
    }

    getSubgraph():string {
        return this.props['grebi:subgraph']
    }

    getLinkUrl():string {
        return `/subgraphs/${this.getSubgraph()}/nodes/${encodeNodeId(this.getNodeId())}`;
    }

    isBoldForQuery(q:string) {
        let allIdentifiers = [
            ...this.getNames().map(p => p.value),
            ...this.getSynonyms().map(p => p.value),
            ...this.getIds().map(p => p.value),
        ];
        return allIdentifiers.indexOf(q) !== -1
    }

    isDeprecated() {
        return false // TODO: only if this is nothing else but an ontology term which is deprecated
    }

    getRefs():Refs {
        return new Refs(this.props['_refs'])
    }

    getProps():{[key:string]:PropVal[]} {
        let res_props = {}
        let keys = Object.keys(this.props)

        let sortOrder = [
            'grebi:name',
            'grebi:synonym',
            'grebi:description',
            'grebi:type'
        ].filter(k => {
            if(keys.indexOf(k) !== -1) {
                keys.splice(keys.indexOf(k), 1)
                return true
            }
        })
        keys = sortOrder.concat(keys)
        for(let k of keys) {
            //if( (k.startsWith('grebi:') || k === '_refs') && k !== 'grebi:type') {
            if(k === '_refs') {
                continue;
            }
            res_props[k] = PropVal.arrFrom(this.props[k])
        }
        return res_props
    }
}

