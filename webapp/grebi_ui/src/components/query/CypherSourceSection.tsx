import { useMemo, useState } from 'react';
import { Box, Typography, IconButton, Collapse } from '@mui/material';
import ExpandMoreIcon from '@mui/icons-material/ExpandMore';
import ContentCopyIcon from '@mui/icons-material/ContentCopy';

import prismjs from "prismjs";
import "prismjs/components/prism-cypher";

export default function CypherSourceSection({ source }) {

  const [expanded, setExpanded] = useState(false);

    let highlightedSource = useMemo<string>(() => {

        return prismjs.highlight(source, prismjs.languages.cypher, 'cypher');

    }, [source]);


  const handleCopy = () => {
    const tempEl = document.createElement('textarea');
    tempEl.value = highlightedSource.replace(/<[^>]+>/g, ''); // strip HTML
    document.body.appendChild(tempEl);
    tempEl.select();
    document.execCommand('copy');
    document.body.removeChild(tempEl);
  };

  return (
    <Box>
      <Box
        onClick={() => setExpanded(!expanded)}
        sx={{
          display: 'flex',
          alignItems: 'center',
          cursor: 'pointer',
          userSelect: 'none',
          mb: 1,
        }}
      >
        <ExpandMoreIcon
          sx={{
            transform: expanded ? 'rotate(0deg)' : 'rotate(-90deg)',
            transition: 'transform 0.3s',
            mr: 1,
          }}
        />
        <Typography variant="h7">Cypher Query</Typography>
      </Box>

      <Collapse in={expanded}>
        <Box
          sx={{
            mb: 2,
            p: 0,
            border: 'none',
            position: 'relative',
            '&:hover .copy-btn': { opacity: 1 },
          }}
        >
          {/* Copy Button */}
          <IconButton
            className="copy-btn"
            onClick={handleCopy}
            size="small"
            sx={{
              position: 'absolute',
              top: 8,
              right: 8,
              opacity: 0,
              transition: 'opacity 0.3s',
              backgroundColor: 'rgba(255,255,255,0.1)',
              '&:hover': { backgroundColor: 'rgba(255,255,255,0.2)' },
            }}
          >
            <ContentCopyIcon fontSize="small" sx={{ color: 'white' }} />
          </IconButton>

          <pre
            style={{
              whiteSpace: 'pre-wrap',
              wordBreak: 'break-all',
              color: 'white',
              fontSize: '0.875rem',
            }}
            dangerouslySetInnerHTML={{ __html: highlightedSource }}
            className="bg-slate-900 p-2 rounded"
          />
        </Box>
      </Collapse>
    </Box>
  );
}

