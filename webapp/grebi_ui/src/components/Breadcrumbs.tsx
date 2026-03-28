import { Link } from "react-router-dom";
import { Fragment } from "react/jsx-runtime";
import MuiBreadcrumbs from '@mui/material/Breadcrumbs';
import { Home, NavigateNext } from "@mui/icons-material";
import SubgraphPicker from "./SubgraphPicker";

export interface BreadcrumbsEntry {
    url:string
    label:string|JSX.Element
}

export default function Breadcrumbs(props:{ subgraph?:string, setSubgraph?:(s:string)=>void, entries: BreadcrumbsEntry[] }) {

    let entries: BreadcrumbsEntry[] = props.entries;
    let subgraph = props.subgraph;

    return <MuiBreadcrumbs
  separator={<NavigateNext fontSize="small" />}
  aria-label="breadcrumb"
>

<Link
  to={subgraph ? `/subgraphs/${subgraph}` : "/"}
  className="hover:text-gray-800 flex items-center"
>
  <Home fontSize="small" />
</Link>

{subgraph && props.setSubgraph && (
  <SubgraphPicker subgraph={subgraph} setSubgraph={props.setSubgraph} compact />
)}

  {entries.map((entry, index) => (
    <Link key={index} to={entry.url} className="hover:text-gray-800">
      {entry.label}
    </Link>
  ))}
</MuiBreadcrumbs>



}