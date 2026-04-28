import { Box, Button, Checkbox, Divider, FormControlLabel, FormGroup, Paper, Typography } from "@mui/material";
import QueryTopic from "../../model/QueryTopic";
import InputBadge from "./InputBadge";
import OutputBadge from "./OutputBadge";

export interface QueryFacetSelection {
  selectedTopics: Set<string>;
  selectedInputs: Set<string>;
  selectedOutputs: Set<string>;
}

export default function QueryFacets({
  topics,
  availableInputs,
  availableOutputs,
  selectedTopics,
  selectedInputs,
  selectedOutputs,
  onTopicsChange,
  onInputsChange,
  onOutputsChange
}: {
  topics: QueryTopic[];
  availableInputs: string[];
  availableOutputs: string[];
  selectedTopics: Set<string>;
  selectedInputs: Set<string>;
  selectedOutputs: Set<string>;
  onTopicsChange: (selected: Set<string>) => void;
  onInputsChange: (selected: Set<string>) => void;
  onOutputsChange: (selected: Set<string>) => void;
}) {
  const topicsByType: Record<string, QueryTopic[]> = {};
  topics.forEach((topic) => {
    if (!topicsByType[topic.type]) {
      topicsByType[topic.type] = [];
    }
    topicsByType[topic.type].push(topic);
  });

  Object.values(topicsByType).forEach((groupedTopics) => {
    groupedTopics.sort((a, b) => a.name.localeCompare(b.name));
  });

  const handleTopicToggle = (topicId: string) => {
    const newSelection = new Set(selectedTopics);
    if (newSelection.has(topicId)) {
      newSelection.delete(topicId);
    } else {
      newSelection.add(topicId);
    }
    onTopicsChange(newSelection);
  };

  const handleTopicTypeToggle = (type: string) => {
    const typeTopics = topicsByType[type];
    const allSelected = typeTopics.every((topic) => selectedTopics.has(topic.id));
    const newSelection = new Set(selectedTopics);

    if (allSelected) {
      typeTopics.forEach((topic) => newSelection.delete(topic.id));
    } else {
      typeTopics.forEach((topic) => newSelection.add(topic.id));
    }

    onTopicsChange(newSelection);
  };

  const toggleValue = (
    value: string,
    selected: Set<string>,
    onChange: (selected: Set<string>) => void
  ) => {
    const newSelection = new Set(selected);
    if (newSelection.has(value)) {
      newSelection.delete(value);
    } else {
      newSelection.add(value);
    }
    onChange(newSelection);
  };

  const clearAllFilters = () => {
    onTopicsChange(new Set());
    onInputsChange(new Set());
    onOutputsChange(new Set());
  };

  const hasActiveFilters = selectedTopics.size > 0 || selectedInputs.size > 0 || selectedOutputs.size > 0;

  return (
    <Paper elevation={1} sx={{ p: 2 }}>
      <Box sx={{ display: "flex", alignItems: "center", justifyContent: "space-between", mb: 2 }}>
        <Typography variant="h6">Filters</Typography>
        {hasActiveFilters && (
          <Button size="small" onClick={clearAllFilters}>
            Clear all
          </Button>
        )}
      </Box>

      <Typography variant="subtitle1" fontWeight="bold" sx={{ mb: 1 }}>
        Topics
      </Typography>
      {Object.entries(topicsByType).map(([type, typeTopics]) => {
        const allSelected = typeTopics.every((topic) => selectedTopics.has(topic.id));
        const someSelected = typeTopics.some((topic) => selectedTopics.has(topic.id));

        return (
          <Box key={type} sx={{ mb: 2 }}>
            <FormControlLabel
              control={
                <Checkbox
                  checked={allSelected}
                  indeterminate={someSelected && !allSelected}
                  onChange={() => handleTopicTypeToggle(type)}
                />
              }
              label={
                <Typography variant="subtitle2" fontWeight="bold">
                  {type}
                </Typography>
              }
            />
            <FormGroup sx={{ ml: 3 }}>
              {typeTopics.map((topic) => (
                <FormControlLabel
                  key={topic.id}
                  control={
                    <Checkbox
                      checked={selectedTopics.has(topic.id)}
                      onChange={() => handleTopicToggle(topic.id)}
                      size="small"
                    />
                  }
                  label={topic.name}
                />
              ))}
            </FormGroup>
          </Box>
        );
      })}

      <Divider sx={{ my: 2 }} />

      <Typography variant="subtitle1" fontWeight="bold" sx={{ mb: 1 }}>
        Inputs
      </Typography>
      <FormGroup>
        {availableInputs.map((inputId) => (
          <FormControlLabel
            key={inputId}
            control={
              <Checkbox
                checked={selectedInputs.has(inputId)}
                onChange={() => toggleValue(inputId, selectedInputs, onInputsChange)}
                size="small"
              />
            }
            label={<InputBadge size="xs">{inputId}</InputBadge>}
          />
        ))}
      </FormGroup>

      <Divider sx={{ my: 2 }} />

      <Typography variant="subtitle1" fontWeight="bold" sx={{ mb: 1 }}>
        Outputs
      </Typography>
      <FormGroup>
        {availableOutputs.map((outputId) => (
          <FormControlLabel
            key={outputId}
            control={
              <Checkbox
                checked={selectedOutputs.has(outputId)}
                onChange={() => toggleValue(outputId, selectedOutputs, onOutputsChange)}
                size="small"
              />
            }
            label={<OutputBadge size="xs">{outputId}</OutputBadge>}
          />
        ))}
      </FormGroup>
    </Paper>
  );
}
