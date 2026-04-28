import { MouseEvent, useState } from "react";
import {
  Badge,
  Box,
  Checkbox,
  CircularProgress,
  FormControlLabel,
  FormGroup,
  IconButton,
  Popover,
  Typography,
} from "@mui/material";
import FilterListIcon from "@mui/icons-material/FilterList";
import InputBadge from "./InputBadge";
import OutputBadge from "./OutputBadge";
import { QueryTemplate } from "../../model/QueryTemplate";
import QueryQuestion from "./QueryQuestion";
import { useNavigate } from "react-router-dom";

type FilterType = "inputs" | "outputs" | null;

function toggleValue(value: string, selected: Set<string>, onChange: (selected: Set<string>) => void) {
  const newSelection = new Set(selected);
  if (newSelection.has(value)) {
    newSelection.delete(value);
  } else {
    newSelection.add(value);
  }
  onChange(newSelection);
}

export default function QueryTable({
  graph,
  queries,
  availableInputs,
  availableOutputs,
  selectedInputs,
  selectedOutputs,
  onInputsChange,
  onOutputsChange,
}: {
  graph?: string | undefined;
  queries: QueryTemplate[] | null;
  availableInputs: string[];
  availableOutputs: string[];
  selectedInputs: Set<string>;
  selectedOutputs: Set<string>;
  onInputsChange: (selected: Set<string>) => void;
  onOutputsChange: (selected: Set<string>) => void;
}) {
  const navigate = useNavigate();
  const [anchorEl, setAnchorEl] = useState<HTMLElement | null>(null);
  const [openFilter, setOpenFilter] = useState<FilterType>(null);

  if (!queries) {
    return <CircularProgress />;
  }

  const openPopover = (event: MouseEvent<HTMLElement>, filterType: FilterType) => {
    setAnchorEl(event.currentTarget);
    setOpenFilter(filterType);
  };

  const closePopover = () => {
    setAnchorEl(null);
    setOpenFilter(null);
  };

  const renderFilterPopover = () => {
    if (!openFilter) {
      return null;
    }

    const isInputs = openFilter === "inputs";
    const options = isInputs ? availableInputs : availableOutputs;
    const selected = isInputs ? selectedInputs : selectedOutputs;
    const onChange = isInputs ? onInputsChange : onOutputsChange;
    const title = isInputs ? "Filter inputs" : "Filter outputs";
    const BadgeComponent = isInputs ? InputBadge : OutputBadge;

    return (
      <Popover
        open={Boolean(anchorEl)}
        anchorEl={anchorEl}
        onClose={closePopover}
        anchorOrigin={{ vertical: "bottom", horizontal: "left" }}
        transformOrigin={{ vertical: "top", horizontal: "left" }}
      >
        <Box sx={{ p: 2, minWidth: 280, maxHeight: 360, overflowY: "auto" }}>
          <Box sx={{ display: "flex", alignItems: "center", justifyContent: "space-between", mb: 1 }}>
            <Typography variant="subtitle2">{title}</Typography>
            {selected.size > 0 && (
              <button
                className="text-sm text-blue-600 hover:underline"
                onClick={() => onChange(new Set())}
                type="button"
              >
                Clear
              </button>
            )}
          </Box>
          <FormGroup>
            {options.map((value) => (
              <FormControlLabel
                key={value}
                control={
                  <Checkbox
                    checked={selected.has(value)}
                    onChange={() => toggleValue(value, selected, onChange)}
                    size="small"
                  />
                }
                label={<BadgeComponent size="xs">{value}</BadgeComponent>}
              />
            ))}
          </FormGroup>
        </Box>
      </Popover>
    );
  };

  return (
    <>
      <table className="w-full border-collapse">
        <thead>
          <tr className="border-b-2 border-gray-200 text-left text-sm text-gray-500">
            <th className="py-2 px-3 font-medium w-48">ID</th>
            <th className="py-2 px-3 font-medium">
              <Box sx={{ display: "flex", alignItems: "center", gap: 0.5 }}>
                <span>Inputs</span>
                <IconButton size="small" onClick={(event) => openPopover(event, "inputs")}>
                  <Badge badgeContent={selectedInputs.size} color="primary" invisible={selectedInputs.size === 0}>
                    <FilterListIcon
                      fontSize="small"
                      color={selectedInputs.size > 0 ? "primary" : "inherit"}
                    />
                  </Badge>
                </IconButton>
              </Box>
            </th>
            <th className="py-2 px-3 font-medium">
              <Box sx={{ display: "flex", alignItems: "center", gap: 0.5 }}>
                <span>Outputs</span>
                <IconButton size="small" onClick={(event) => openPopover(event, "outputs")}>
                  <Badge badgeContent={selectedOutputs.size} color="primary" invisible={selectedOutputs.size === 0}>
                    <FilterListIcon
                      fontSize="small"
                      color={selectedOutputs.size > 0 ? "primary" : "inherit"}
                    />
                  </Badge>
                </IconButton>
              </Box>
            </th>
            <th className="py-2 px-3 font-medium">Example</th>
          </tr>
        </thead>
        <tbody>
          {queries.length === 0 && (
            <tr>
              <td colSpan={4} className="py-6 px-3 text-sm text-gray-500">
                No queries match the current filters.
              </td>
            </tr>
          )}
          {queries.map((template, rowIndex) => (
            <tr
              key={template.id}
              className={`border-b border-gray-100 hover:bg-gray-100 transition-colors group cursor-pointer ${rowIndex % 2 === 1 ? "bg-gray-50" : ""}`}
              onClick={() => {
                const example = template.examples?.[0];
                if (example) {
                  const qs = new URLSearchParams(example.params).toString();
                  navigate(`/graphs/${graph}/queries/${template.id}?${qs}`);
                } else {
                  navigate(`/graphs/${graph}/queries/${template.id}`);
                }
              }}
            >
              <td className="py-2 px-3 font-mono text-sm text-gray-600 group-hover:text-blue-600 align-top">
                {template.id}
              </td>
              <td className="py-2 px-3 align-top">
                <div className="flex flex-wrap gap-1">
                  {(template.params || []).map((param) => (
                    <InputBadge key={param.param_id} size="xs">
                      {param.param_id}
                    </InputBadge>
                  ))}
                </div>
              </td>
              <td className="py-2 px-3 align-top">
                <div className="flex flex-wrap gap-1">
                  {(template.result_columns || []).map((column) => (
                    <OutputBadge key={column.column_id} size="xs">
                      {column.column_id}
                    </OutputBadge>
                  ))}
                </div>
              </td>
              <td className="py-2 px-3 align-top">
                <QueryQuestion
                  graph={graph!}
                  template={template}
                  exampleIndex={0}
                  fontSize="0.95rem"
                  readOnly={true}
                />
              </td>
            </tr>
          ))}
        </tbody>
      </table>
      {renderFilterPopover()}
    </>
  );
}
