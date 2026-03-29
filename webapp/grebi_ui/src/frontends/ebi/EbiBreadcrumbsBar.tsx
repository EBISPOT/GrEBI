import { useLocation, useNavigate } from "react-router-dom";
import Breadcrumbs, { BreadcrumbsEntry } from "../../components/Breadcrumbs";

export default function EbiBreadcrumbsBar({
  graph,
  entries,
}: {
  graph?: string;
  entries: BreadcrumbsEntry[];
}) {
  const loc = useLocation();
  const navigate = useNavigate();

  function setGraph(sg: string) {
    const { pathname, search, hash } = loc;
    const newPath = pathname.replace(/graphs\/[^/]+/, `graphs/${sg}`);
    navigate(`${newPath}${search}${hash}`);
  }

  return (
    <div className="bg-stone-100 pt-1 pl-2 pr-2 pb-1 flex flex-row justify-between items-center">
      <Breadcrumbs
        graph={graph}
        setGraph={setGraph}
        entries={entries}
      />
    </div>
  );
}
