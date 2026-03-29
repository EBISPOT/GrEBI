import EbiBreadcrumbsBar from "../EbiBreadcrumbsBar";
import SearchInterface from "../../../components/SearchInterface";
import { useParams } from "react-router-dom";

export default function EbiSearchPage() {

  let params = useParams()
  const graph: string = params.graph as string;

  let breadcrumbs = [
    { url: `/graphs`, label: "Graphs" },
    { url: `/graphs/${graph}/search`, label: "Search" },
  ]

  return (
    <div>
      <EbiBreadcrumbsBar graph={graph} entries={breadcrumbs} />
      <main className="container mx-auto px-4 h-fit my-8">
        <SearchInterface graph={graph} />
      </main>
    </div>
  );
}
