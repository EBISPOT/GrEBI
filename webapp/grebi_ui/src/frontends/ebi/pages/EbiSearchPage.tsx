import EbiBreadcrumbsBar from "../EbiBreadcrumbsBar";
import SearchInterface from "../../../components/SearchInterface";
import { Link, useParams } from "react-router-dom";

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
        <div className="text-sm text-gray-600 mt-8 px-1">
          Have a list of identifiers?&thinsp;
          <Link className="link-default" to={`/graphs/${graph}/lookup`}>Look them up in bulk</Link>.
        </div>
      </main>
    </div>
  );
}
