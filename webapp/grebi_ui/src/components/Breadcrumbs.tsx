import { Link } from "react-router-dom";
import { Fragment } from "react/jsx-runtime";
import MuiBreadcrumbs from '@mui/material/Breadcrumbs';
import { NavigateNext } from "@mui/icons-material";

export interface BreadcrumbsEntry {
    url:string
    label:string|JSX.Element
}

export default function Breadcrumbs(props:{ entries: BreadcrumbsEntry[] }) {

    let entries: BreadcrumbsEntry[] = props.entries;

    if(entries.length === 0) {
        return <Fragment/>
    }
    return <MuiBreadcrumbs
  separator={<NavigateNext fontSize="small" />}
  aria-label="breadcrumb"
>
  {entries.map((entry, index) => (
    <Link key={index} to={entry.url} className="hover:text-gray-800">
      {entry.label}
    </Link>
  ))}
</MuiBreadcrumbs>



}