import { Fragment } from "react";
import { useParams } from "react-router-dom";
import EbiBreadcrumbsBar from "../EbiBreadcrumbsBar";
import { FTP_BASE, LATEST_RELEASE, releaseFiles } from "../../../app/ftp";

/** The files of the latest release that concern the graph in the URL. */
export default function EbiDownloadsPage() {
  const params = useParams();
  const graph = params.graph as string;
  const files = releaseFiles(graph);

  return (
    <Fragment>
      <EbiBreadcrumbsBar graph={graph} entries={[
        { url: `/graphs`, label: "Graphs" },
        { url: `/graphs/${graph}/downloads`, label: "Downloads" }
      ]} />
      <main className="container mx-auto px-4 my-8">
        <div className="text-2xl font-bold my-6">
          Downloading Knowledge Graph Exports
        </div>
        <p className="px-1 mb-4">
          Releases are published on the EMBL-EBI FTP at&thinsp;
          <a className="link-default" href={`${FTP_BASE}/`} rel="noopener noreferrer" target="_blank">{`${FTP_BASE}/`}</a>.
          The files below are those of <code>{graph}</code> in the latest release,&thinsp;
          <a className="link-default" href={`${LATEST_RELEASE}/`} rel="noopener noreferrer" target="_blank">latest/</a>;
          earlier releases are in the dated folders beside it. Sizes and dates are listed there.
        </p>
        <table className="w-full text-sm border border-gray-200 rounded-lg overflow-hidden">
          <thead>
            <tr className="bg-gray-50 text-left text-gray-600 border-b border-gray-200">
              <th className="py-2 px-3 font-medium">Description</th>
              <th className="py-2 px-3 font-medium">File</th>
              <th className="py-2 px-3 font-medium">Format</th>
            </tr>
          </thead>
          <tbody>
            {files.map((f, i) => (
              <tr key={f.file} className={`border-b border-gray-100 ${i % 2 === 1 ? "bg-gray-50" : ""}`}>
                <td className="py-2 px-3 text-gray-700">{f.description}</td>
                <td className="py-2 px-3 whitespace-nowrap">
                  <a className="link-default font-mono" href={f.url} rel="noopener noreferrer" target="_blank">{f.file}</a>
                </td>
                <td className="py-2 px-3 text-gray-600">{f.format}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </main>
    </Fragment>
  );
}
