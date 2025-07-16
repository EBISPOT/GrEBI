import { Checkbox, FormControlLabel, ThemeProvider } from "@mui/material";
import { Fragment, useCallback, useEffect, useRef, useState } from "react";
import { Link, useNavigate, useSearchParams } from "react-router-dom";
import { getPaginated } from "../app/api";
import { theme } from "../app/mui";
import { joinSearchParams, randomString } from "../app/util";
import React from "react";
import GraphNodeRef from "../model/GraphNodeRef";
import encodeNodeId from "../encodeNodeId";
import { DatasourceTags } from "./DatasourceTag";
import NodeTypeChip from "./NodeTypeChip";

interface SearchBoxEntry {
  linkUrl: string;
  li: JSX.Element;
}

export default function NodeSelectorBox({
  subgraph,
  placeholder,
  selectedNode,
  onNodeSelect,
  additionalParams,
}: {
  subgraph: string;
  placeholder?: string;
  selectedNode?: GraphNodeRef;
  onNodeSelect: (node: GraphNodeRef) => void;
  additionalParams?: URLSearchParams;
}) {
  const [searchParams, setSearchParams] = useSearchParams();
  const navigate = useNavigate();

  const [suggestions, setSuggestions] = useState<GraphNodeRef[]>([]);
  const [loading, setLoading] = useState<boolean>(false);
  const [query, setQuery] = useState<string>("");
  const [isFocused, setIsFocused] = useState(false);
  const [arrowKeySelectedN, setArrowKeySelectedN] = useState<number | undefined>(undefined);

  const mounted = useRef(false);
  const cancelPromisesRef = useRef(false);
  const searchTokenRef = useRef("");
  const debounceRef = useRef<number | null>(null);

  useEffect(() => {
    mounted.current = true;
    return () => {
      mounted.current = false;
      if (debounceRef.current) clearTimeout(debounceRef.current);
    };
  }, []);

  useEffect(() => {
    if (debounceRef.current) clearTimeout(debounceRef.current);

    if (!query.trim()) {
      setSuggestions([]);
      setLoading(false);
      return;
    }

    setLoading(true);
    debounceRef.current = window.setTimeout(async () => {
      cancelPromisesRef.current = false;
      setArrowKeySelectedN(undefined);
      const token = randomString();
      searchTokenRef.current = token;

      try {
        const nodes = await getPaginated<any>(
          `api/v1/subgraphs/${subgraph}/search?${joinSearchParams(
            new URLSearchParams({ q: query, resolve: "false", size: "5", lang: "en" }),
            additionalParams
          )}`
        );

        if (cancelPromisesRef.current || !mounted.current) return;

        if (searchTokenRef.current === token) {
          setSuggestions(nodes.elements.map((node) => new GraphNodeRef(node)));
        }
      } catch (err) {
        console.error("Failed to fetch suggestions:", err);
      } finally {
        if (mounted.current) setLoading(false);
      }
    }, 300);

    return () => {
      cancelPromisesRef.current = true;
      if (debounceRef.current) clearTimeout(debounceRef.current);
    };
  }, [query, subgraph, additionalParams]);

  const handleSelectNode = (node: GraphNodeRef) => {
    setQuery("");
    setSuggestions([]);
    onNodeSelect(node);
  };

  return (
    <div className="w-full self-center">
      <div className="flex space-x-4 items-center mb-2">
        <div className="relative grow">
          <input
            type="text"
            autoComplete="off"
            placeholder={
              selectedNode ? selectedNode.getName() : placeholder || "Type to search..."
            }
            className={`input-default text-lg pl-3 ${
              query !== "" && isFocused ? "rounded-b-sm shadow-input" : ""
            }`}
            onBlur={() => setTimeout(() => mounted.current && setIsFocused(false), 500)}
            onFocus={() => setIsFocused(true)}
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            onKeyDown={(ev) => {
              if (ev.key === "Enter") {
                if (arrowKeySelectedN !== undefined && arrowKeySelectedN < suggestions.length) {
                  handleSelectNode(suggestions[arrowKeySelectedN]!);
                }
              } else if (ev.key === "ArrowDown") {
                setArrowKeySelectedN((prev) =>
                  prev !== undefined ? Math.min(prev + 1, suggestions.length - 1) : 0
                );
              } else if (ev.key === "ArrowUp") {
                setArrowKeySelectedN((prev) =>
                  prev !== undefined ? Math.max(prev - 1, 0) : suggestions.length - 1
                );
              }
            }}
          />
          {loading && (
            <div className="spinner-default w-7 h-7 absolute right-3 top-2.5 z-10" />
          )}
          <ul
            className={
              query !== "" && (isFocused || suggestions.length > 0)
                ? "list-none bg-white text-neutral-dark border-2 border-neutral-dark shadow-input rounded-b-md w-full absolute left-0 top-12 z-10"
                : "hidden"
            }
          >
            {suggestions.map((entry, i) => {
              const name = entry.getName();
              const type = entry.extractType();
              return (
                <li
                  key={entry.getId().value}
                  className={
                    "py-1 px-3 leading-7 hover:bg-link-light hover:cursor-pointer" +
                    (arrowKeySelectedN === i ? " bg-link-light" : "")
                  }
                  onClick={() => handleSelectNode(entry)}
                >
                  <div className="flex justify-between">
                    <div className="truncate flex-auto" title={name}>
                      {name}
                      {type && <NodeTypeChip type={type} />}
                    </div>
                    <div className="truncate flex-initial ml-2 text-right">
                      <DatasourceTags dss={entry.getDatasources()} />
                    </div>
                  </div>
                </li>
              );
            })}
          </ul>
        </div>
      </div>
    </div>
  );
}
