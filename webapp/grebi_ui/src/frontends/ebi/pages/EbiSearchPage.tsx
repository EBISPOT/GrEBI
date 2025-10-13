import EbiHeader from "../EbiHeader";
import SearchInterface from "../../../components/SearchInterface";
import { useParams } from "react-router-dom";

export default function EbiSearchPage() {

  let params = useParams()
  const subgraph: string = params.subgraph as string;

  let breadcrumbs = [
    { url: `/subgraphs/${subgraph}/search`, label: "Search" },
  ]

  return (
    <div>
      <EbiHeader section="search" subgraph={subgraph} showBreadcrumbsBar={true} breadcrumbs={breadcrumbs} />
      <main className="container mx-auto px-4 h-fit my-8">
        <SearchInterface subgraph={subgraph} />
      </main>
    </div>
  );
}
