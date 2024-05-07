import Reified from "./Reified"

export default class GraphNode {

    props:any

    constructor(props:any) {
        this.props = props
    }

    getNames():Reified<string>[] {
        return [
            ...(this.props['ols:label'] || []).map(Reified.from),
            ...(this.props['impc:name'] || []).map(Reified.from),
            ...(this.props['monarch:name'] || []).map(Reified.from)
        ];
    }

    getName():Reified<string> {
        return this.getNames()[0] || this.getId()
    }

    getDescriptions():Reified<string>[] {
        return (this.props['rdfs:comment'] || []).map(Reified.from)
    }

    getDescription():Reified<string> {
        return this.getDescriptions()[0] || Reified.from('')
    }

    getNodeId():string {
        return this.props['grebi:nodeId']
    }

    getId():Reified<string> {
        if(this.props['ols:curie']){
            return Reified.from(this.props['ols:curie'][0])
        }
        return Reified.from(this.props['grebi:nodeId'])
    }

    getIds():Reified<string>[] {
        return (this.props['id'] || []).map(Reified.from)
    }

    extractType():'Gene'|'Disease'|'Phenotype'|undefined {

        let types:string[] = this.props['grebi:type'].map(Reified.from).map(r => r.value)

        if(types.indexOf('impc:MouseGene') !== -1) {
            return 'Gene'
        }
        if(types.indexOf('ols:Class') !== -1) {
            let ancestors:any[] = this.props['ols:hirarchicalAncestors']
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
}