import React, { useState, useEffect, useRef, Fragment } from "react";
import { useSearchParams, useNavigate } from "react-router-dom";
import { IconButton } from "@mui/material";
import CloseIcon from "@mui/icons-material/Close";

import { getPaginated } from "../app/api";
import { joinSearchParams, randomString } from "../app/util";

import GraphNodeRef from "../model/GraphNodeRef";
import { DatasourceTags } from "./DatasourceTag";
import NodeTypeChip from "./NodeTypeChip";

interface NodeSelectorBoxProps {
  graph: string;
  placeholder?: string;
  selectedNode?: GraphNodeRef;
  onNodeSelect: (node: GraphNodeRef) => void;
  onClear: () => void;
  additionalParams?: URLSearchParams;
}

export default function NodeSelectorBox({
  graph,
  placeholder,
  selectedNode,
  onNodeSelect,
  onClear,
  additionalParams,
}: NodeSelectorBoxProps) {
  const [searchParams, setSearchParams] = useSearchParams();
  const navigate = useNavigate();

  const [suggestions, setSuggestions] = useState<GraphNodeRef[]>([]);
  const [loading, setLoading] = useState(false);
  const [query, setQuery] = useState("");
  const [isFocused, setIsFocused] = useState(false);
  const [arrowKeySelectedN, setArrowKeySelectedN] = useState<number>();

  const mounted = useRef(false);
  const cancelPromisesRef = useRef(false);
  const searchTokenRef = useRef("");
  const debounceRef = useRef<number>();
  const inputRef = useRef<HTMLInputElement>(null);

  // track mounted state
  useEffect(() => {
    mounted.current = true;
    return () => {
      mounted.current = false;
      if (debounceRef.current) clearTimeout(debounceRef.current);
    };
  }, []);

  // fetch suggestions
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
          `api/v1/graphs/${graph}/search?${joinSearchParams(
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
  }, [query, graph, additionalParams]);

  const handleSelectNode = (node: GraphNodeRef) => {
    setQuery("");
    setSuggestions([]);
    onNodeSelect(node);
    // blur input and clear focus state
    inputRef.current?.blur();
    setIsFocused(false);
  };

  return (
    <div className="w-full self-center">
      <div className="flex space-x-4 items-center mb-2">
        <div className="relative grow">
          <input
            ref={inputRef}
            type="text"
            autoComplete="off"
            placeholder={!selectedNode && !query ? placeholder || "Type to search…" : ""}
            value={query !== "" ? query : selectedNode?.getName() ?? ""}
            className={`
              input-default
              text-lg
              pl-3
              pr-8
              placeholder:text-neutral-500
              ${selectedNode && !query ? "bg-slate-100 text-neutral-900" : ""}
              ${query !== "" && isFocused ? "rounded-b-sm shadow-input" : ""}
            `}
            onBlur={() => setTimeout(() => mounted.current && setIsFocused(false), 200)}
            onFocus={() => {
              setIsFocused(true);
              // if focusing into a selected node, let them type anew
              if (selectedNode && !query) {
                onClear();
                setSuggestions([]);
              }
            }}
            onChange={(e) => {
              setQuery(e.target.value);
              if (selectedNode) onClear();
            }}
            onKeyDown={(ev) => {
              if (ev.key === "Enter") {
                if (arrowKeySelectedN != null && arrowKeySelectedN < suggestions.length) {
                  handleSelectNode(suggestions[arrowKeySelectedN]!);
                }
              } else if (ev.key === "ArrowDown") {
                setArrowKeySelectedN((prev) =>
                  prev != null ? Math.min(prev + 1, suggestions.length - 1) : 0
                );
              } else if (ev.key === "ArrowUp") {
                setArrowKeySelectedN((prev) =>
                  prev != null ? Math.max(prev - 1, 0) : suggestions.length - 1
                );
              }
            }}
          />

        {/* Clear button, vertically centered on the right */}
        {selectedNode && !query && (
        <div className="absolute inset-y-0 right-0 flex items-center pr-2">
            <IconButton
            size="small"
            onClick={() => {
                onClear();
                setQuery("");
                setSuggestions([]);
            }}
            >
            <CloseIcon fontSize="small" />
            </IconButton>
        </div>
        )}

          {/* Loading spinner */}
          {loading && (
            <div className="spinner-default w-7 h-7 absolute right-3 top-2.5 z-10" />
          )}

          {/* Suggestions dropdown */}
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
