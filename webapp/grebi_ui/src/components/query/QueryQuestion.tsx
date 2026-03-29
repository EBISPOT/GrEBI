import React, { useState, useEffect, useRef, useMemo, Fragment, useCallback } from "react";
import { useNavigate } from "react-router-dom";
import { IconButton } from "@mui/material";
import PlayArrowIcon from "@mui/icons-material/PlayArrow";

import { getPaginated } from "../../app/api";
import { joinSearchParams, randomString } from "../../app/util";

import GraphNodeRef from "../../model/GraphNodeRef";
import { QueryTemplate, Parameter, Example } from "../../model/QueryTemplate";
import { DatasourceTags } from "../DatasourceTag";
import NodeTypeChip from "../NodeTypeChip";

interface QueryQuestionProps {
  graph: string;
  template: QueryTemplate;
  exampleIndex?: number;
  onAllParamsFilled?: (params: Record<string, string>) => void;
  fontSize?: string;
  autoNavigate?: boolean;
  readOnly?: boolean;
}

interface ParamState {
  query: string;
  selectedNode: GraphNodeRef | null;
  suggestions: GraphNodeRef[];
  loading: boolean;
  isFocused: boolean;
  arrowKeySelectedN: number | undefined;
  textValue: string;
  editing: boolean;
}

export default function QueryQuestion({
  graph,
  template,
  exampleIndex,
  onAllParamsFilled,
  fontSize = "1rem",
  autoNavigate = true,
  readOnly = false,
}: QueryQuestionProps) {
  const navigate = useNavigate();
  const question = template.question || "";
  const params = template.params || [];
  const examples = template.examples || [];
  const currentExample: Example | undefined = examples[exampleIndex ?? 0];

  // Parse question into segments: text parts, param placeholders {id}, and result refs [text]{col_id}
  const segments = useMemo(() => {
    const result: { type: "text" | "param" | "result"; value: string; displayText?: string }[] = [];
    const regex = /\[([^\]]+)\]\{([^}]+)\}|\{([^}]+)\}/g;
    let lastIndex = 0;
    let match;
    while ((match = regex.exec(question)) !== null) {
      if (match.index > lastIndex) {
        result.push({ type: "text", value: question.slice(lastIndex, match.index) });
      }
      if (match[1] != null) {
        result.push({ type: "result", value: match[2], displayText: match[1] });
      } else {
        result.push({ type: "param", value: match[3] });
      }
      lastIndex = regex.lastIndex;
    }
    if (lastIndex < question.length) {
      result.push({ type: "text", value: question.slice(lastIndex) });
    }
    return result;
  }, [question]);

  // Read-only mode: render plain text with example values, no interactivity
  if (readOnly) {
    return (
      <span className="inline" style={{ fontSize, lineHeight: "1.8em" }}>
        {segments.map((seg, i) => {
          if (seg.type === "text") {
            return <span key={i} style={{ whiteSpace: "pre-wrap" }}>{seg.value}</span>;
          }
          if (seg.type === "result") {
            return <span key={i} className="font-semibold">{seg.displayText || seg.value}</span>;
          }
          // param: show example value or placeholder
          const paramId = seg.value;
          const exampleValue = currentExample?.title || currentExample?.params?.[paramId];
          const param = params.find((p) => p.param_id === paramId);
          const display = exampleValue || param?.param_name || paramId;
          return <span key={i} className="font-semibold text-blue-600">{display}</span>;
        })}
      </span>
    );
  }

  // State per parameter
  const [paramStates, setParamStates] = useState<Record<string, ParamState>>({});
  const inputRefs = useRef<Record<string, HTMLInputElement | null>>({});
  const debounceRefs = useRef<Record<string, number>>({});
  const cancelRefs = useRef<Record<string, boolean>>({});
  const searchTokenRefs = useRef<Record<string, string>>({});
  const [navigatedViaAutocomplete, setNavigatedViaAutocomplete] = useState(false);

  // Initialize param states
  useEffect(() => {
    const initial: Record<string, ParamState> = {};
    for (const p of params) {
      initial[p.param_id] = {
        query: "",
        selectedNode: null,
        suggestions: [],
        loading: false,
        isFocused: false,
        arrowKeySelectedN: undefined,
        textValue: "",
        editing: true,
      };
    }
    setParamStates(initial);
    setNavigatedViaAutocomplete(false);
  }, [template.id, exampleIndex]);

  const updateParamState = useCallback(
    (paramId: string, update: Partial<ParamState>) => {
      setParamStates((prev) => ({
        ...prev,
        [paramId]: { ...prev[paramId], ...update },
      }));
    },
    []
  );

  // Check if all params are filled
  const allParamsFilled = useMemo(() => {
    for (const p of params) {
      const state = paramStates[p.param_id];
      if (!state) return false;
      if (p.param_type === "SourceId") {
        if (!state.selectedNode) return false;
      } else {
        if (!state.textValue) return false;
      }
    }
    return true;
  }, [paramStates, params]);

  // Whether any param was filled via autocomplete selection
  const anyFilledViaAutocomplete = useRef(false);

  // Build URL params from current state
  const buildUrlParams = useCallback(() => {
    const result: Record<string, string> = {};
    for (const p of params) {
      const state = paramStates[p.param_id];
      if (!state) return null;
      if (p.param_type === "SourceId") {
        if (!state.selectedNode) return null;
        result[p.param_id] = state.selectedNode.getId().value;
      } else {
        if (!state.textValue) return null;
        result[p.param_id] = state.textValue;
      }
    }
    return result;
  }, [paramStates, params]);

  // Navigate when all params are filled via autocomplete
  useEffect(() => {
    if (allParamsFilled && anyFilledViaAutocomplete.current && !navigatedViaAutocomplete) {
      const urlParams = buildUrlParams();
      if (urlParams && autoNavigate) {
        setNavigatedViaAutocomplete(true);
        const qs = new URLSearchParams(urlParams).toString();
        navigate(`/graphs/${graph}/queries/${template.id}?${qs}`);
      }
      if (urlParams && onAllParamsFilled) {
        onAllParamsFilled(urlParams);
      }
    }
  }, [allParamsFilled, navigatedViaAutocomplete, buildUrlParams, autoNavigate]);

  // Fetch suggestions for SourceId params
  const fetchSuggestions = useCallback(
    (paramId: string, q: string, param: Parameter) => {
      if (debounceRefs.current[paramId]) clearTimeout(debounceRefs.current[paramId]);

      if (!q.trim()) {
        updateParamState(paramId, { suggestions: [], loading: false });
        return;
      }

      updateParamState(paramId, { loading: true });
      cancelRefs.current[paramId] = false;

      debounceRefs.current[paramId] = window.setTimeout(async () => {
        const token = randomString();
        searchTokenRefs.current[paramId] = token;

        try {
          const additionalParams = param.param_opts
            ? new URLSearchParams(param.param_opts)
            : undefined;
          const nodes = await getPaginated<any>(
            `api/v1/graphs/${graph}/search?${joinSearchParams(
              new URLSearchParams({ q, resolve: "false", size: "5", lang: "en" }),
              additionalParams
            )}`
          );
          if (cancelRefs.current[paramId]) return;
          if (searchTokenRefs.current[paramId] === token) {
            updateParamState(paramId, {
              suggestions: nodes.elements.map((n: any) => new GraphNodeRef(n)),
              loading: false,
            });
          }
        } catch {
          updateParamState(paramId, { loading: false });
        }
      }, 300);
    },
    [graph, updateParamState]
  );

  const handleSelectNode = useCallback(
    (paramId: string, node: GraphNodeRef) => {
      anyFilledViaAutocomplete.current = true;
      updateParamState(paramId, {
        selectedNode: node,
        query: "",
        suggestions: [],
        isFocused: false,
        editing: false,
      });
      inputRefs.current[paramId]?.blur();
    },
    [updateParamState]
  );

  const handleGoClick = useCallback(() => {
    const urlParams = buildUrlParams();
    if (urlParams) {
      const qs = new URLSearchParams(urlParams).toString();
      navigate(`/graphs/${graph}/queries/${template.id}?${qs}`);
    }
  }, [buildUrlParams, navigate, graph, template.id]);

  const getPlaceholder = (paramId: string): string => {
    if (currentExample?.params?.[paramId]) {
      // Resolve the example value to a human-readable name
      return currentExample.title || currentExample.params[paramId];
    }
    const param = params.find((p) => p.param_id === paramId);
    return param?.param_name || paramId;
  };

  return (
    <span
      className="inline"
      style={{ fontSize, lineHeight: "1.8em" }}
    >
      {segments.map((seg, i) => {
        if (seg.type === "text") {
          return (
            <span key={i} style={{ whiteSpace: "pre-wrap" }}>
              {seg.value}
            </span>
          );
        }

        if (seg.type === "result") {
          return (
            <span key={i} className="font-semibold">
              {seg.displayText || seg.value}
            </span>
          );
        }

        const paramId = seg.value;
        const param = params.find((p) => p.param_id === paramId);
        const state = paramStates[paramId];

        if (!param || !state) {
          return (
            <span key={i} className="text-red-500">
              {`{${paramId}}`}
            </span>
          );
        }

        if (param.param_type === "SourceId") {
          return (
            <span key={i} className="relative inline-block" style={{ verticalAlign: "baseline" }}>
              <>
              <input
                ref={(el) => {
                  inputRefs.current[paramId] = el;
                }}
                type="text"
                autoComplete="off"
                placeholder={getPlaceholder(paramId)}
                value={
                  state.query !== ""
                    ? state.query
                    : state.selectedNode?.getName() ?? ""
                }
                className={`
                  bg-transparent
                  outline-none px-0
                  placeholder:text-blue-500
                  text-blue-600 font-semibold
                  cursor-text
                  hover:bg-blue-50 focus:bg-blue-50
                  rounded
                  ${state.selectedNode && !state.query ? "text-green-700" : ""}
                `}
                style={{
                  fontSize: "inherit",
                  lineHeight: "inherit",
                  width: ((state.query || state.selectedNode?.getName() || getPlaceholder(paramId)).length + 1) + "ch",
                  maxWidth: "400px",
                }}
                onFocus={() => {
                  updateParamState(paramId, { isFocused: true });
                  if (state.selectedNode && !state.query) {
                    updateParamState(paramId, { selectedNode: null, suggestions: [] });
                  }
                }}
                onBlur={() =>
                  setTimeout(() => updateParamState(paramId, { isFocused: false }), 200)
                }
                onChange={(e) => {
                  const val = e.target.value;
                  updateParamState(paramId, { query: val, selectedNode: null });
                  fetchSuggestions(paramId, val, param);
                }}
                onKeyDown={(ev) => {
                  if (ev.key === "Enter") {
                    if (
                      state.arrowKeySelectedN != null &&
                      state.arrowKeySelectedN < state.suggestions.length
                    ) {
                      handleSelectNode(paramId, state.suggestions[state.arrowKeySelectedN]);
                    }
                  } else if (ev.key === "ArrowDown") {
                    ev.preventDefault();
                    updateParamState(paramId, {
                      arrowKeySelectedN:
                        state.arrowKeySelectedN != null
                          ? Math.min(state.arrowKeySelectedN + 1, state.suggestions.length - 1)
                          : 0,
                    });
                  } else if (ev.key === "ArrowUp") {
                    ev.preventDefault();
                    updateParamState(paramId, {
                      arrowKeySelectedN:
                        state.arrowKeySelectedN != null
                          ? Math.max(state.arrowKeySelectedN - 1, 0)
                          : state.suggestions.length - 1,
                    });
                  }
                }}
              />

              {state.loading && (
                <span className="spinner-default w-4 h-4 absolute right-1 top-1/2 -translate-y-1/2" />
              )}

              {/* Suggestions dropdown */}
              {state.query !== "" && state.isFocused && (
                <ul className="list-none bg-white text-neutral-dark border border-gray-300 shadow-lg rounded-b-md w-full absolute left-0 top-full z-50 max-h-60 overflow-y-auto text-sm"
                  style={{ fontSize: "0.85rem", lineHeight: "1.5" }}
                >
                  {state.suggestions.map((entry, si) => {
                    const name = entry.getName();
                    const type = entry.extractType();
                    return (
                      <li
                        key={entry.getId().value}
                        className={
                          "py-1 px-3 leading-7 hover:bg-blue-50 hover:cursor-pointer" +
                          (state.arrowKeySelectedN === si ? " bg-blue-50" : "")
                        }
                        onClick={() => handleSelectNode(paramId, entry)}
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
              )}
              </>
            </span>
          );
        }

        // string or float param
        return (
          <span key={i} className="inline-block" style={{ verticalAlign: "baseline" }}>
            <input
              ref={(el) => { inputRefs.current[paramId] = el; }}
              type={param.param_type === "float" ? "number" : "text"}
              placeholder={getPlaceholder(paramId)}
              value={state.textValue}
              className={`
                bg-transparent
                outline-none px-0
                placeholder:text-blue-500
                text-blue-600 font-semibold
                cursor-text
                hover:bg-blue-50 focus:bg-blue-50
                rounded
                ${state.textValue ? "text-green-700" : ""}
              `}
              style={{
                fontSize: "inherit",
                lineHeight: "inherit",
                width: ((state.textValue || getPlaceholder(paramId)).length + 1) + "ch",
                maxWidth: "300px",
              }}
              onChange={(e) => updateParamState(paramId, { textValue: e.target.value })}
              onKeyDown={(ev) => {
                if (ev.key === "Enter" && allParamsFilled) {
                  handleGoClick();
                }
              }}
            />
          </span>
        );
      })}

      {/* Show go button when all params are filled (for non-autocomplete fills) */}
      {allParamsFilled && !navigatedViaAutocomplete && (
        <IconButton
          size="small"
          onClick={handleGoClick}
          sx={{
            ml: 0.5,
            verticalAlign: "baseline",
            color: "rgb(37 99 235)",
            "&:hover": { backgroundColor: "rgb(219 234 254)" },
          }}
        >
          <PlayArrowIcon fontSize="small" />
        </IconButton>
      )}
    </span>
  );
}
