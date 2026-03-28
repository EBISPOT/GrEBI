
import { useState, useEffect, Fragment, useMemo } from "react";
import { get } from "../../app/api";
import { CircularProgress } from "@mui/material";
import InputBadge from "./InputBadge";
import OutputBadge from "./OutputBadge";
import { QueryTemplate } from "../../model/QueryTemplate";
import QueryTopic from "../../model/QueryTopic";
import QueryQuestion from "./QueryQuestion";
import { useNavigate } from "react-router-dom";

export default function QueryTable({
    subgraph,
    selectedTopics
}:{
    subgraph?:string|undefined,
    selectedTopics?: Set<string>
}) {

  let [topics, setTopics] = useState<QueryTopic[]|null>(null);
  let [queries, setQueries] = useState<QueryTemplate[]|null>(null);
  const navigate = useNavigate();

    useEffect(() => {
        get<QueryTemplate[]>(`api/v1/subgraphs/${subgraph}/query_templates`).then(r => setQueries(r));
    }, [subgraph])

    useEffect(() => {
        get<QueryTopic[]>(`api/v1/topics`).then(r => setTopics(r));
    }, [])

    // Filter queries by selected topics
    const filteredQueries = useMemo(() => {
        if (!queries) return null;
        if (!selectedTopics || selectedTopics.size === 0) return queries;
        
        return queries.filter(query => {
            if (!query.topics || query.topics.length === 0) return false;
            return query.topics.some(topicId => selectedTopics.has(topicId));
        });
    }, [queries, selectedTopics]);

    if(!filteredQueries || !topics) {
        return <CircularProgress />
    }

    return <table className="w-full border-collapse">
        <thead>
            <tr className="border-b-2 border-gray-200 text-left text-sm text-gray-500">
                <th className="py-2 px-3 font-medium w-48">ID</th>
                <th className="py-2 px-3 font-medium">Inputs</th>
                <th className="py-2 px-3 font-medium">Outputs</th>
                <th className="py-2 px-3 font-medium">Example</th>
            </tr>
        </thead>
        <tbody>
            {filteredQueries.map((template, rowIndex) => (
                <tr
                    key={template.id}
                    className={`border-b border-gray-100 hover:bg-gray-100 transition-colors group cursor-pointer ${rowIndex % 2 === 1 ? "bg-gray-50" : ""}`}
                    onClick={() => {
                        const example = template.examples?.[0];
                        if (example) {
                            const qs = new URLSearchParams(example.params).toString();
                            navigate(`/subgraphs/${subgraph}/queries/${template.id}?${qs}`);
                        } else {
                            navigate(`/subgraphs/${subgraph}/queries/${template.id}`);
                        }
                    }}
                >
                    <td
                        className="py-2 px-3 font-mono text-sm text-gray-600 group-hover:text-blue-600 align-top"
                    >
                        {template.id}
                    </td>
                    <td className="py-2 px-3 align-top">
                        <div className="flex flex-wrap gap-1">
                            {(template.params || []).map((p) => (
                                <InputBadge key={p.param_id} size="xs">{p.param_id}</InputBadge>
                            ))}
                        </div>
                    </td>
                    <td className="py-2 px-3 align-top">
                        <div className="flex flex-wrap gap-1">
                            {(template.result_columns || []).map((c) => (
                                <OutputBadge key={c.column_id} size="xs">{c.column_id}</OutputBadge>
                            ))}
                        </div>
                    </td>
                    <td className="py-2 px-3 align-top">
                        <QueryQuestion
                            subgraph={subgraph!}
                            template={template}
                            exampleIndex={0}
                            fontSize="0.95rem"
                            readOnly={true}
                        />
                    </td>
                </tr>
            ))}
        </tbody>
    </table>;
}
