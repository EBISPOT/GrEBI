import React from "react";
import { Checkbox, FormControlLabel, FormGroup, Typography } from "@mui/material";
import { FilterAlt, Visibility } from "@mui/icons-material";
import DatasourceSelector from "../DatasourceSelector";

export default function GraphViewControls({
  datasources,
  dsEnabled,
  setDsEnabled,
  onMouseoverDs,
  onMouseoutDs,
  edgeTypes,
  hiddenEdgeTypes,
  onToggleEdgeType,
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
  edgeTypes: string[];
  hiddenEdgeTypes: Set<string>;
  onToggleEdgeType: (edgeType: string) => void;
  onShowAllEdgeTypes: () => void;
  onHideAllEdgeTypes: () => void;
  onMouseoverEdgeType?: (et: string) => void;
  onMouseoutEdgeType?: (et: string) => void;
}) {
  const allEdgeTypesVisible = hiddenEdgeTypes.size === 0;
  const noneEdgeTypesVisible = hiddenEdgeTypes.size === edgeTypes.length;

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
              Edge Types ({edgeTypes.length - hiddenEdgeTypes.size}/{edgeTypes.length})
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
          <FormGroup>
            {edgeTypes.map((et) => (
              <FormControlLabel
                key={et}
                control={
                  <Checkbox
                    size="small"
                    checked={!hiddenEdgeTypes.has(et)}
                    onChange={() => onToggleEdgeType(et)}
                    sx={{ py: 0, px: 0.5 }}
                  />
                }
                label={
                  <Typography
                    variant="body2"
                    sx={{
                      fontFamily: "monospace", fontSize: "10px", lineHeight: 1.1,
                      cursor: "pointer", borderRadius: "3px", px: 0.5,
                      '&:hover': { outline: '2px solid #1976d2', outlineOffset: '1px' },
                    }}
                    onMouseEnter={() => onMouseoverEdgeType && onMouseoverEdgeType(et)}
                    onMouseLeave={() => onMouseoutEdgeType && onMouseoutEdgeType(et)}
                  >
                    {et}
                  </Typography>
                }
                sx={{ mx: 0, my: -0.25 }}
              />
            ))}
          </FormGroup>
          </div>
        </div>
      )}
    </div>
  );
}
