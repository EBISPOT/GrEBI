import { Link } from "react-router-dom";
import { Fragment } from "react/jsx-runtime";
import MuiBreadcrumbs from '@mui/material/Breadcrumbs';
import { Home, NavigateNext } from "@mui/icons-material";
import GraphPicker from "./GraphPicker";

export interface BreadcrumbsEntry {
    url:string
    label:string|JSX.Element
}

export default function Breadcrumbs(props:{ graph?:string, setGraph?:(s:string)=>void, entries: BreadcrumbsEntry[] }) {

    let entries: BreadcrumbsEntry[] = props.entries;

    return <MuiBreadcrumbs
  separator={<NavigateNext fontSize="small" />}
  aria-label="breadcrumb"
>

<Link
  to={"/"}
  className="hover:text-gray-800 flex items-center"
>
  <Home fontSize="small" />
</Link>

  {entries.length > 0 && (
    <Link to={entries[0].url} className="hover:text-gray-800">
      {entries[0].label}
    </Link>
  )}

  {props.graph && props.setGraph && (
    <GraphPicker graph={props.graph} setGraph={props.setGraph} compact />
  )}

  {entries.slice(1).map((entry, index) => (
    <Link key={index} to={entry.url} className="hover:text-gray-800">
      {entry.label}
    </Link>
  ))}
</MuiBreadcrumbs>



}