import React from "react";
import { Checkbox, FormControlLabel, FormGroup, Typography } from "@mui/material";
import { FilterAlt, Visibility } from "@mui/icons-material";
import DatasourceSelector from "../DatasourceSelector";

/** A category's edge types in the filters, each with its number of edges. */
export interface EdgeTypeGroup {
  category: { id: string; label: string };
  edgeTypes: { edgeType: string; count: number }[];
}

const formatter = Intl.NumberFormat("en", { notation: "compact" });

export default function GraphViewControls({
  datasources,
  dsEnabled,
  setDsEnabled,
  onMouseoverDs,
  onMouseoutDs,
  edgeTypeGroups,
  hiddenEdgeTypes,
  onToggleEdgeType,
  onSetEdgeTypesHidden,
  onShowAllEdgeTypes,
  onHideAllEdgeTypes,
  onMouseoverEdgeType,
  onMouseoutEdgeType,
}: {
  datasources: string[];
  dsEnabled: string[];
  setDsEnabled: (ds: string[]) => void;
  onMouseoverDs?: (ds: string) => void;
  onMouseoutDs?: (ds: string) => void;
  /** The edge types to list, by category */
  edgeTypeGroups: EdgeTypeGroup[];
  hiddenEdgeTypes: Set<string>;
  onToggleEdgeType: (edgeType: string) => void;
  /** Shows or hides a whole category's edge types */
  onSetEdgeTypesHidden?: (edgeTypes: string[], hidden: boolean) => void;
  onShowAllEdgeTypes: () => void;
  onHideAllEdgeTypes: () => void;
  onMouseoverEdgeType?: (et: string) => void;
  onMouseoutEdgeType?: (et: string) => void;
}) {
  const edgeTypes = edgeTypeGroups.flatMap((g) => g.edgeTypes.map((e) => e.edgeType));
  const visibleCount = edgeTypes.filter((et) => !hiddenEdgeTypes.has(et)).length;
  const allEdgeTypesVisible = visibleCount === edgeTypes.length;
  const noneEdgeTypesVisible = visibleCount === 0;
  const grouped = edgeTypeGroups.length > 1;

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: "4px", height: "100%" }}>
      {/* Datasource filter */}
      <div style={{ border: "1px solid #e0e0e0", borderRadius: "6px", padding: "6px", flex: "1 1 50%", minHeight: 0, display: "flex", flexDirection: "column" }}>
        <div style={{ display: "flex", alignItems: "center", marginBottom: "4px", flexShrink: 0 }}>
          <FilterAlt sx={{ mr: 0.5, color: "#888", fontSize: "14px" }} />
          <Typography variant="caption" fontWeight="bold" sx={{ fontSize: "11px" }}>
            Datasources
          </Typography>
        </div>
        <div style={{ overflowY: "auto", flex: 1, minHeight: 0 }}>
          <DatasourceSelector
            datasources={datasources}
            dsEnabled={dsEnabled}
            setDsEnabled={setDsEnabled}
            onMouseoverDs={onMouseoverDs}
            onMouseoutDs={onMouseoutDs}
            orientation="vertical"
          />
        </div>
      </div>

      {/* Edge type filter */}
      {edgeTypes.length > 0 && (
        <div style={{ border: "1px solid #e0e0e0", borderRadius: "6px", padding: "6px", flex: "1 1 50%", minHeight: 0, display: "flex", flexDirection: "column" }}>
          <div style={{ display: "flex", alignItems: "center", marginBottom: "4px", flexShrink: 0 }}>
            <Visibility sx={{ mr: 0.5, color: "#888", fontSize: "14px" }} />
            <Typography variant="caption" fontWeight="bold" sx={{ fontSize: "11px" }}>
              Edge Types ({visibleCount}/{edgeTypes.length})
            </Typography>
          </div>
          {edgeTypes.length > 1 && (
            <div style={{ display: "flex", gap: "4px", marginBottom: "4px", flexShrink: 0 }}>
              <button
                onClick={onShowAllEdgeTypes}
                disabled={allEdgeTypesVisible}
                style={{
                  fontSize: "11px", padding: "1px 6px", borderRadius: "4px",
                  border: "1px solid #ccc", background: allEdgeTypesVisible ? "#f5f5f5" : "#fff",
                  color: allEdgeTypesVisible ? "#aaa" : "#555", cursor: allEdgeTypesVisible ? "default" : "pointer",
                }}
              >All</button>
              <button
                onClick={onHideAllEdgeTypes}
                disabled={noneEdgeTypesVisible}
                style={{
                  fontSize: "11px", padding: "1px 6px", borderRadius: "4px",
                  border: "1px solid #ccc", background: noneEdgeTypesVisible ? "#f5f5f5" : "#fff",
                  color: noneEdgeTypesVisible ? "#aaa" : "#555", cursor: noneEdgeTypesVisible ? "default" : "pointer",
                }}
              >None</button>
            </div>
          )}
          <div style={{ overflowY: "auto", flex: 1, minHeight: 0 }}>
            {edgeTypeGroups.map((group) => {
              const types = group.edgeTypes.map((e) => e.edgeType);
              const shown = types.filter((et) => !hiddenEdgeTypes.has(et)).length;
              return (
                <div key={group.category.id} style={{ marginBottom: grouped ? "4px" : 0 }}>
                  {/* The category, with a checkbox for all of its edge types */}
                  {grouped && (
                    <FormControlLabel
                      control={
                        <Checkbox
                          size="small"
                          checked={shown === types.length}
                          indeterminate={shown > 0 && shown < types.length}
                          onChange={() => onSetEdgeTypesHidden?.(types, shown === types.length)}
                          inputProps={{ "aria-label": group.category.label }}
                          sx={{ py: 0, px: 0.5 }}
                        />
                      }
                      label={
                        <Typography variant="caption" sx={{ fontSize: "10px", fontWeight: "bold", textTransform: "uppercase", letterSpacing: "0.03em", color: "#666" }}>
                          {group.category.label}
                        </Typography>
                      }
                      sx={{ mx: 0, my: -0.25 }}
                    />
                  )}
                  <FormGroup sx={grouped ? { pl: 1.5 } : undefined}>
                    {group.edgeTypes.map(({ edgeType: et, count }) => (
                      <FormControlLabel
                        key={et}
                        control={
                          <Checkbox
                            size="small"
                            checked={!hiddenEdgeTypes.has(et)}
                            onChange={() => onToggleEdgeType(et)}
                            inputProps={{ "aria-label": et }}
                            sx={{ py: 0, px: 0.5 }}
                          />
                        }
                        label={
                          <span style={{ display: "flex", alignItems: "baseline", gap: "4px", minWidth: 0 }}>
                            <Typography
                              variant="body2"
                              title={et}
                              sx={{
                                fontFamily: "monospace", fontSize: "10px", lineHeight: 1.1,
                                cursor: "pointer", borderRadius: "3px", px: 0.5,
                                overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap", minWidth: 0,
                                '&:hover': { outline: '2px solid #1976d2', outlineOffset: '1px' },
                              }}
                              onMouseEnter={() => onMouseoverEdgeType && onMouseoverEdgeType(et)}
                              onMouseLeave={() => onMouseoutEdgeType && onMouseoutEdgeType(et)}
                            >
                              {et}
                            </Typography>
                            <span style={{ fontSize: "10px", color: "#999", flexShrink: 0 }}>{formatter.format(count)}</span>
                          </span>
                        }
                        sx={{ mx: 0, my: -0.25, maxWidth: "100%", '& .MuiFormControlLabel-label': { minWidth: 0, overflow: "hidden" } }}
                      />
                    ))}
                  </FormGroup>
                </div>
              );
            })}
          </div>
        </div>
      )}
    </div>
  );
}
