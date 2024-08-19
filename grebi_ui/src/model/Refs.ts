
export class Ref {
	nodeId:string
	type:string[]
	id:string[]
	name:string[]

	constructor(obj:any) {
		this.nodeId = obj['grebi:nodeId']
		this.type = obj['grebi:type']
		this.id = obj['id']
		this.name = obj['grebi:name']
	}
}

export default class Refs {

	refs:{ [key:string]:Ref }

	constructor(refs:any) {
		if(refs)
			this.refs = {...refs}
		else
			this.refs = {}
	}

	mergeWith(refs:any):Refs {
		if(refs)
			return new Refs({ ...this.refs, refs })
		else
			return new Refs({ ...this.refs })
	}

	get(iri:string):Ref|undefined {
		return this.refs[iri] && new Ref(this.refs[iri])
	}
}
