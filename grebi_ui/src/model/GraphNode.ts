import { pickBestDisplayName } from "../app/util";
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

    getName():string {
        return pickBestDisplayName(this.getNames().map(n => n.value)) || this.getId().value
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