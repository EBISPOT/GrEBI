import GraphNodeRef from "./GraphNodeRef"

export default class Refs {

	refs:{ [key:string]:GraphNodeRef }

	constructor(refs:any) {
		if(refs)
			this.refs = {...refs}
		else
			this.refs = {}
	}

	mergeWith(refs:any):Refs {
		if(refs)
			return new Refs({ ...this.refs, ...refs })
		else
			return new Refs({ ...this.refs })
	}

	get(iri:string):GraphNodeRef|undefined {
		return this.refs[iri] && new GraphNodeRef(this.refs[iri])
	}
}
