
/**
 * A materialised table as /api/v1/graphs/{graph}/tables lists it: the whole
 * result of a query template (parameterised) or of a materialised query with
 * no parameters (standalone), published with the release as files.
 */
export default interface MaterialisedTable {

    id:string
    graph:string
    title?:string
    description?:string
    kind:"parameterised"|"standalone"
    /** The columns of the published files */
    columns:string[]
    num_rows?:number

}
