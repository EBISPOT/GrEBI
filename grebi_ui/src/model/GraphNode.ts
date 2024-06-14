import { pickBestDisplayName } from "../app/util";
import encodeNodeId from "../encodeNodeId";
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

    getSynonyms():PropVal[] {
        return PropVal.arrFrom(this.props['grebi:synonym'] || []);
    }

    getName():string {
        return pickBestDisplayName(this.getNames().map(n => n.value)) || this.getId().value
    }

    getDescriptions():PropVal[] {
        return PropVal.arrFrom(this.props['grebi:description'])
    }

    getDescription():string|undefined {
        return PropVal.arrFrom(this.getDescriptions())[0]?.value
    }

    getNodeId():string {
        return this.props['grebi:nodeId']
    }

    getLinkUrl():string {
        return `/nodes/${encodeNodeId(this.getNodeId())}`;
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

    extractType():{long:string,short:string}|undefined {

        let types:string[] = PropVal.arrFrom(this.props['grebi:type']).map(t => t.value)

        if(types.indexOf('impc:MouseGene') !== -1) {
            return {long:'Gene',short:'Gene'}
        }
        if(types.indexOf('biolink:Gene') !== -1) {
            return {long:'Gene',short:'Gene'}
        }
        if(types.indexOf('gwas:SNP') !== -1) {
            return {long:'SNP',short:'SNP'}
        }
        if(types.indexOf('reactome:ReferenceDNASequence') !== -1) {
            return {long:'DNA',short:'DNA'}
        }
        if(types.indexOf('reactome:Person') !== -1) {
            return {long:'Person',short:'Person'}
        }
        if(types.indexOf('ols:Class') !== -1) {
            let ancestors:any[] = PropVal.arrFrom(this.props['ols:directAncestor']).map(a => a.value)
            if(ancestors.indexOf("chebi:36080") !== -1) {
                return {long:'Protein',short:'Protein'}
            }
            if(ancestors.indexOf("chebi:24431") !== -1) {
                return {long:'Chemical',short:'Chemical'}
            }
            if(ancestors.indexOf("mondo:0000001") !== -1 || ancestors.indexOf("efo:0000408") !== -1) {
                return {long:'Disease',short:'Disease'}
            }
            return {long:'Ontology Class',short:'Class'}
        }
        if(types.indexOf('ols:Property') !== -1) {
            return {long:'Ontology Property',short:'Property'}
        }
        if(types.indexOf('ols:Individual') !== -1) {
            return {long:'Ontology Individual',short:'Individual'}
        }

    }

    getDatasources():string[] {
        return this.props['grebi:datasources'] || []
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

