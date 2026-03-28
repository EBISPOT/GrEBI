import { useMemo, useState } from 'react';
import { IconButton } from '@mui/material';
import ContentCopyIcon from '@mui/icons-material/ContentCopy';
import prismjs from "prismjs";
import "prismjs/components/prism-cypher";
import "prismjs/components/prism-python";
import { copyToClipboard } from '../../app/util';

interface SourceTab {
  title: string;
  source: string;
  lang: string;
}

export default function TabbedSourceView({ tabs }: { tabs: SourceTab[] }) {
  const [activeTab, setActiveTab] = useState(0);

  const highlighted = useMemo(() => {
    return tabs.map(tab =>
      prismjs.highlight(tab.source, prismjs.languages[tab.lang.toLowerCase()], tab.lang.toLowerCase())
    );
  }, [tabs]);

  if (tabs.length === 0) return null;

  return (
    <div className="flex flex-col h-full">
      <div className="flex bg-slate-800 rounded-t">
        {tabs.map((tab, i) => (
          <button
            key={i}
            onClick={() => setActiveTab(i)}
            className={`px-4 py-2 text-sm font-medium transition-colors ${
              activeTab === i
                ? 'text-white bg-slate-900'
                : 'text-slate-400 hover:text-slate-200'
            }`}
          >
            {tab.title}
          </button>
        ))}
      </div>
      <div className="relative flex-1 min-h-0">
        <IconButton
          onClick={() => copyToClipboard(tabs[activeTab].source)}
          size="small"
          title="Copy to clipboard"
          sx={{
            position: 'absolute',
            top: 8,
            right: 8,
            color: 'rgba(255,255,255,0.5)',
            '&:hover': { color: 'white', backgroundColor: 'rgba(255,255,255,0.1)' },
          }}
        >
          <ContentCopyIcon fontSize="small" />
        </IconButton>
        <pre
          style={{
            whiteSpace: 'pre-wrap',
            wordBreak: 'break-all',
            color: 'white',
            fontSize: '0.875rem',
            margin: 0,
            height: '100%',
            maxHeight: '400px',
            overflowY: 'auto',
            boxSizing: 'border-box',
          }}
          dangerouslySetInnerHTML={{ __html: highlighted[activeTab] }}
          className="bg-slate-900 p-4 rounded-b"
        />
      </div>
    </div>
  );
}
