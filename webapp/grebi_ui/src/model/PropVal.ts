
class PropVal {


    datasources:string[]
    props:any
    value:any

    constructor(datasources:string[], props:any, value:any) {
        this.datasources = datasources
        this.props = props;
        this.value = value;
    }


    /**
     * The language of a translated value, or undefined for a value in the
     * graph's own language. The dataload keeps a literal in a language other
     * than English as a reified value carrying grebi:lang; English and
     * untagged literals are plain strings.
     */
    language():string|undefined {
        // from() keeps a reified value's properties under grebi:properties
        let bag = this.props && this.props['grebi:properties'] ? this.props['grebi:properties'] : this.props
        let lang = bag ? bag['grebi:lang'] : undefined
        if(Array.isArray(lang)) {
            lang = lang[0]
        }
        return typeof lang === 'string' ? lang.toLowerCase() : undefined
    }

    public static from(src:any):PropVal {

        if(src === undefined || src === null)
            return new PropVal([], {}, '')

        if(src instanceof PropVal) {
            return src
        }

        if(typeof src !== 'object') {
            return new PropVal([], {}, src)
        }

        let ds = src['grebi:datasources']
        let value = src['grebi:value']

        if(ds === undefined || value === undefined) {
            return new PropVal([], {}, src)
        }

        if(typeof value === 'object' && value['grebi:value'] !== undefined) {
            // reified
            let reif_props = Object.assign({}, value)
            delete reif_props['grebi:value']
            return new PropVal(ds, reif_props, value['grebi:value'])

        } else {
            // not reified
            return new PropVal(ds, {}, value)
        }
    }

    public static arrFrom(src:any):PropVal[] {
        if(src !== undefined && src !== null) {
            if(Array.isArray(src)) {
                return src.map(PropVal.from)
            } else {
                return [PropVal.from(src)]
            }
        } else {
            return []
        }
    } 

    public static anyFrom(src:any):PropVal|PropVal[] {
        return Array.isArray(src) ? PropVal.arrFrom(src) : PropVal.from(src)
    }

}

export default PropVal;
