import { Fragment, useMemo, useState } from 'react';
import { Box, Dialog, DialogTitle, DialogContent, IconButton } from '@mui/material';
import OpenInNewIcon from '@mui/icons-material/OpenInNew';
import ContentCopyIcon from '@mui/icons-material/ContentCopy';
import CloseIcon from '@mui/icons-material/Close';

import prismjs from "prismjs";
import "prismjs/components/prism-cypher";
import "prismjs/components/prism-python";
import { copyToClipboard } from '../../app/util';

export default function SourceCodeSection({ title, source, lang }) {

  const [open, setOpen] = useState(false);

    let highlightedSource = useMemo<string>(() => {

        return prismjs.highlight(source, prismjs.languages[lang.toLowerCase()], lang.toLowerCase());

    }, [source]);


  const handleCopy = () => {
    copyToClipboard(source);
  };

  return (
    <Fragment>
      <button
        onClick={() => setOpen(true)}
        className="inline-flex items-center gap-1.5 px-3 py-1.5 text-sm font-medium text-gray-700 bg-white border border-gray-300 rounded-md hover:bg-gray-50 transition-colors"
      >
        {title}
        <OpenInNewIcon sx={{ fontSize: 16 }} />
      </button>

      <Dialog open={open} onClose={() => setOpen(false)} maxWidth="md" fullWidth>
        <DialogTitle sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', pb: 1 }}>
          {title}
          <div>
            <IconButton onClick={handleCopy} size="small" title="Copy to clipboard" sx={{ mr: 0.5 }}>
              <ContentCopyIcon fontSize="small" />
            </IconButton>
            <IconButton onClick={() => setOpen(false)} size="small">
              <CloseIcon fontSize="small" />
            </IconButton>
          </div>
        </DialogTitle>
        <DialogContent>
          <pre
            style={{
              whiteSpace: 'pre-wrap',
              wordBreak: 'break-all',
              color: 'white',
              fontSize: '0.875rem',
            }}
            dangerouslySetInnerHTML={{ __html: highlightedSource }}
            className="bg-slate-900 p-4 rounded"
          />
        </DialogContent>
      </Dialog>
    </Fragment>
  );
}

