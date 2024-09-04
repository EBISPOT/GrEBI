
export default class DatasourceSet {

    datasources:Set<string> = new Set()
    dsEnabled:Set<string> = new Set()

    constructor(datasources:string[]) {
        this.datasources = new Set(datasources)
    }


}
