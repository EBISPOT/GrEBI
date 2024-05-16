import PropVal from "./PropVal";
import Refs from "./Refs";

export default class GraphNode {

    props:any

    constructor(props:any) {
        this.props = props
    }

    getNames():PropVal[] {
        return PropVal.arrFrom(this.props['grebi:name'] || []);
    }

    getName():PropVal {
        return this.getNames()[0] || this.getId()
    }

    getDescriptions():PropVal[] {
        return PropVal.arrFrom(this.props['rdfs:comment'])
    }

    getDescription():PropVal|undefined {
        return PropVal.arrFrom(this.getDescriptions())[0]
    }

    getNodeId():string {
        return this.props['grebi:nodeId']
    }

    getId():PropVal {
        if(this.props['ols:curie']){
            return PropVal.arrFrom(this.props['ols:curie'])[0]
        }
        return PropVal.from(this.props['grebi:nodeId'])
    }

    getIds():PropVal[] {
        return PropVal.arrFrom(this.props['id'])
    }

    extractType():string|undefined {

        let types:string[] = PropVal.arrFrom(this.props['grebi:type']).map(t => t.value)

        if(types.indexOf('impc:MouseGene') !== -1) {
            return 'Gene'
        }
        if(types.indexOf('gwas:SNP') !== -1) {
            return 'SNP'
        }
        if(types.indexOf('ols:Class') !== -1) {
            let ancestors:any[] = this.props['ols:hierarchicalAncestors']
            return 'Class'
        }
        if(types.indexOf('ols:Property') !== -1) {
            return 'Property'
        }
        if(types.indexOf('ols:Individual') !== -1) {
            return 'Individual'
        }

    }

    getDatasources():string[] {
        return this.props['grebi:datasources'] || []
    }

    isBold() { // idk yet
        return false
    }

    isDeprecated() {
        return false // TODO: only if this is nothing else but an ontology term which is deprecated
    }

    getRefs():Refs {
        return new Refs(this.props['_refs'])
    }

    getProps():{[key:string]:PropVal[]} {
        let res_props = {}
        for(let k of Object.keys(this.props)) {
            if( (k.startsWith('grebi:') || k === '_refs') && k !== 'grebi:type') {
                continue;
            }
            res_props[k] = PropVal.arrFrom(this.props[k])
        }
        return res_props
    }
}