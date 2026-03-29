import { QueryTemplate } from "../../model/QueryTemplate";

export default function query2code(queryTemplate:QueryTemplate, graph:string, params:Record<string,any>) {

    let apiUrl = process.env.REACT_APP_APIURL;

    let exampleParams = queryTemplate.examples.length > 0 ? queryTemplate.examples[0].params : {};

    let baseUrl = `${apiUrl}api/v1/graphs/${graph}/query/${queryTemplate.id}`;

    let queryString = queryTemplate.params.map(p => `${p.param_id}=${encodeURIComponent(exampleParams[p.param_id] ?? "")}`).join("&");

    let curl = [
        `curl -G '${baseUrl}.csv' \\`,
        ...queryTemplate.params.map((p, i) =>
            `  --data-urlencode '${p.param_id}=${exampleParams[p.param_id] ?? ""}'${i < queryTemplate.params.length - 1 ? " \\\\" : ""}`
        ),
    ]

    let python = [
        "import requests",
        "import json",
        "import pandas as pd",
        "from io import StringIO",
        "",
        `def ${queryTemplate.id}(${queryTemplate.params.map(p => p.param_id).join(", ")}):`,
        `    url = f"${baseUrl}.csv?${queryTemplate.params.map(
                p => `${p.param_id}={${p.param_id}}`)
                    .join("&")}"`,
        "    return pd.read_csv(StringIO(requests.get(url).text))",
        "",
        `print(${queryTemplate.id}(${Object.keys(exampleParams).map(key => JSON.stringify(exampleParams[key])).join(", ")}))`
    ]

    let r = [
        "library(httr)",
        "library(readr)",
        "",
        `${queryTemplate.id} <- function(${queryTemplate.params.map(p => p.param_id).join(", ")}) {`,
        `  url <- "${baseUrl}.csv"`,
        `  response <- GET(url, query = list(`,
        ...queryTemplate.params.map((p, i) =>
            `    ${p.param_id} = ${p.param_id}${i < queryTemplate.params.length - 1 ? "," : ""}`
        ),
        "  ))",
        "  read_csv(content(response, as = \"text\", encoding = \"UTF-8\"))",
        "}",
        "",
        `print(${queryTemplate.id}(${Object.keys(exampleParams).map(key => JSON.stringify(exampleParams[key])).join(", ")}))`
    ]

    return {
        "cURL": curl.join("\n"),
        "Python": python.join("\n"),
        "R": r.join("\n"),
    }


}

