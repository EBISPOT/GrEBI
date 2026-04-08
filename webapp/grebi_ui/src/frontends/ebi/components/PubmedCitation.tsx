import { useEffect, useState } from "react";
import { registerPmid, getGeneration } from "./pubmedRegistry";

export default function PubmedCitation(props: Record<string, string> & { children?: any }) {
  const pmid = props.id || "";
  const [num, setNum] = useState<number | null>(null);

  useEffect(() => {
    if (pmid) {
      setNum(registerPmid(pmid));
    }
  }, [pmid]);

  if (!pmid || num === null) return <>{props.children}</>;

  return (
    <>
      <a
        href={`#ref-${num}`}
        className="text-gray-400 no-underline hover:underline"
        style={{ fontSize: "0.75em", verticalAlign: "super" }}
      >
        [{num}]
      </a>
      {props.children}
    </>
  );
}
