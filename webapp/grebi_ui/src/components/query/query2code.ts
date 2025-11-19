import { QueryTemplate } from "../../model/QueryTemplate";

export default function query2code(queryTemplate:QueryTemplate, params:Record<string,any>) {

    let apiUrl = process.env.REACT_APP_APIURL;

    let exampleParams = queryTemplate.examples.length > 0 ? queryTemplate.examples[0].params : {};

    let python = [
        "import requests",
        "import json",
        "import pandas as pd",
        "from io import StringIO",
        "",
        `def ${queryTemplate.id}(${queryTemplate.params.map(p => p.param_id).join(", ")}):`,
        `    url = f"${apiUrl}api/v1/subgraphs/${queryTemplate.subgraphs[0]}/query/${queryTemplate.id}.csv?${queryTemplate.params.map(
                p => `${p.param_id}={${p.param_id}}`)
                    .join("&")}"`,
        "    return pd.read_csv(StringIO(requests.get(url).text))",
        "",
        `print(${queryTemplate.id}(${Object.keys(exampleParams).map(key => JSON.stringify(exampleParams[key])).join(", ")}))`
    ]

    return {
        "Python": python.join("\n")
    }


}

