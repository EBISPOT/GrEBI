
class PropVal {


    datasources:string[]
    props:any
    value:any

    constructor(datasources:string[], props:any, value:any) {
        this.datasources = datasources
        this.props = props;
        this.value = value;
    }


    public static from(src:any):PropVal {

        if(!src)
            return src

        if(typeof src !== 'object') {
            return new PropVal([], {}, src)
        }

        let ds = src['grebi:datasources']
        let value = src['grebi:value']

        if(ds === undefined || value === undefined) {
            throw new Error('missing ds or value in ' + JSON.stringify(src))
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
