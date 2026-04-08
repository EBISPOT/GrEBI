import React, { useState, useEffect } from "react";
import { subscribe, getRegisteredRefs } from "./pubmedRegistry";

function LinkOutIcon() {
  return (
    <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 20 20" fill="currentColor"
      style={{ width: "0.85em", height: "0.85em", display: "inline", verticalAlign: "text-bottom" }}>
      <path fillRule="evenodd" d="M4.25 5.5a.75.75 0 00-.75.75v8.5c0 .414.336.75.75.75h8.5a.75.75 0 00.75-.75v-4a.75.75 0 011.5 0v4A2.25 2.25 0 0112.75 17h-8.5A2.25 2.25 0 012 14.75v-8.5A2.25 2.25 0 014.25 4h5a.75.75 0 010 1.5h-5zm7.25-.182a.75.75 0 01.75-.75h3.5a.75.75 0 01.75.75v3.5a.75.75 0 01-1.5 0V6.56l-5.22 5.22a.75.75 0 11-1.06-1.06l5.22-5.22h-2.19a.75.75 0 01-.75-.75z" clipRule="evenodd" />
    </svg>
  );
}

function FormatCitation({ entry }: { entry: any }) {
  const parts: React.ReactNode[] = [];
  if (entry.authors) parts.push(entry.authors + ". ");
  if (entry.title) parts.push(entry.title + " ");
  if (entry.journal) parts.push(<em key="j">{entry.journal}</em>);
  let suffix = "";
  if (entry.year) suffix += ` (${entry.year})`;
  if (entry.volume) {
    suffix += ` ${entry.volume}`;
    if (entry.issue) suffix += `(${entry.issue})`;
  }
  if (entry.pages) suffix += `: ${entry.pages}`;
  suffix += ".";
  if (entry.doi) suffix += ` doi: ${entry.doi}`;
  parts.push(suffix);
  return <>{parts}</>;
}

export default function PubmedReferences() {
  const [, setTick] = useState(0);
  useEffect(() => subscribe(() => setTick((t) => t + 1)), []);
  const refs = getRegisteredRefs();

  if (refs.length === 0) return null;

  return (
    <section className="mt-8 border-t-2 border-purple-700 pt-4">
      <ol className="list-none p-0 m-0 space-y-1">
        {refs.map(({ pmid, num, entry }) => (
          <li key={pmid} id={`ref-${num}`} className="text-sm leading-relaxed">
            <span className="font-semibold text-gray-600 mr-1">[{num}]</span>
            <a
              href={`https://pubmed.ncbi.nlm.nih.gov/${pmid}/`}
              className="text-inherit hover:text-purple-700 hover:underline"
              style={{ color: "inherit", textDecoration: "none" }}
              target="_blank"
              rel="noopener noreferrer"
            >
              {entry ? <FormatCitation entry={entry} /> : <>PMID: {pmid}</>}
            </a>
          </li>
        ))}
      </ol>
    </section>
  );
}
