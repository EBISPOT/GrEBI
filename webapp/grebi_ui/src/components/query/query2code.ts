import { QueryTemplate } from "../../model/QueryTemplate";
import sharedQuery2code from "../../../../query2code.mjs";

export default function query2code(queryTemplate:QueryTemplate, graph:string, params:Record<string,any>) {
    const apiUrl = (process.env.REACT_APP_APIURL || "").replace(/\/+$/, "");
    const exampleParams = queryTemplate.examples.length > 0 ? queryTemplate.examples[0].params : {};
    const paramIds = queryTemplate.params.map(p => p.param_id);
    return sharedQuery2code(apiUrl, graph, queryTemplate.id, paramIds, exampleParams);
}

