import { Box, Checkbox, FormControlLabel, FormGroup, Paper, Typography } from "@mui/material";
import QueryTopic from "../../model/QueryTopic";

export interface QueryFacetSelection {
  selectedTopics: Set<string>;
}

export default function QueryFacets({
  topics,
  selectedTopics,
  onSelectionChange
}: {
  topics: QueryTopic[];
  selectedTopics: Set<string>;
  onSelectionChange: (selected: Set<string>) => void;
}) {
  // Group topics by type
  const topicsByType: Record<string, QueryTopic[]> = {};
  topics.forEach(topic => {
    if (!topicsByType[topic.type]) {
      topicsByType[topic.type] = [];
    }
    topicsByType[topic.type].push(topic);
  });

  // Sort topics alphabetically within each type
  Object.values(topicsByType).forEach(topics => {
    topics.sort((a, b) => a.name.localeCompare(b.name));
  });

  const handleToggle = (topicId: string) => {
    const newSelection = new Set(selectedTopics);
    if (newSelection.has(topicId)) {
      newSelection.delete(topicId);
    } else {
      newSelection.add(topicId);
    }
    onSelectionChange(newSelection);
  };

  const handleTypeToggle = (type: string) => {
    const typeTopics = topicsByType[type];
    const allSelected = typeTopics.every(t => selectedTopics.has(t.id));
    const newSelection = new Set(selectedTopics);
    
    if (allSelected) {
      // Unselect all of this type
      typeTopics.forEach(t => newSelection.delete(t.id));
    } else {
      // Select all of this type
      typeTopics.forEach(t => newSelection.add(t.id));
    }
    onSelectionChange(newSelection);
  };

  return (
    <Paper elevation={1} sx={{ p: 2 }}>
      <Typography variant="h6" sx={{ mb: 2 }}>
        Filter by Topic
      </Typography>
      {Object.entries(topicsByType).map(([type, typeTopics]) => {
        const allSelected = typeTopics.every(t => selectedTopics.has(t.id));
        const someSelected = typeTopics.some(t => selectedTopics.has(t.id));
        
        return (
          <Box key={type} sx={{ mb: 2 }}>
            <FormControlLabel
              control={
                <Checkbox
                  checked={allSelected}
                  indeterminate={someSelected && !allSelected}
                  onChange={() => handleTypeToggle(type)}
                />
              }
              label={
                <Typography variant="subtitle1" fontWeight="bold">
                  {type}
                </Typography>
              }
            />
            <FormGroup sx={{ ml: 3 }}>
              {typeTopics.map(topic => (
                <FormControlLabel
                  key={topic.id}
                  control={
                    <Checkbox
                      checked={selectedTopics.has(topic.id)}
                      onChange={() => handleToggle(topic.id)}
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
    </Paper>
  );
}
