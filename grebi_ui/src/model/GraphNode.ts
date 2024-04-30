
export default class GraphNode {

    props:any

    constructor(props:any) {
        this.props = props
    }

    getNames() {
        return Array.from(new Set([
            ...(this.props['ols:label'] || []),
            ...(this.props['impc:name'] || []),
            ...(this.props['monarch:name'] || [])
        ]))
    }

    getName() {
        return this.getNames()[0] || this.getId()
    }

    getDescriptions() {
        return Array.from(new Set([
            ...(this.props['rdfs:comment'] || [])
        ]))
    }

    getDescription() {
        return this.getDescriptions()[0] || ''
    }

    getId() {
        if(this.props['ols:curie']){
            return this.props['ols:curie'][0]
        }
        return this.props['grebi:nodeId']
    }

    getIds() {
        return this.props['id']
    }

    extractType():'Gene'|'Disease'|'Phenotype'|undefined {

        let types:string[] = this.props['grebi:type']

        if(types.indexOf('impc:MouseGene') !== -1) {
            return 'Gene'
        }
        if(types.indexOf('ols:Class') !== -1) {
            let ancestors:any[] = this.props['ols:hirarchicalAncestors']
        }
    }

    getDatasources() {
        return this.props['grebi:datasources'] || []
    }

    isBold() { // idk yet
        return false
    }

    isDeprecated() {
        return false // TODO: only if this is nothing else but an ontology term which is deprecated
    }
}