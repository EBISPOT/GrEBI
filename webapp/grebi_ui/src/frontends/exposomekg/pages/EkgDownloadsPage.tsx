

import { Fragment } from "react";
import DataTable, { Column } from "../../../components/datatable/DataTable";
import EkgHeader from "../EkgHeader";
import React from "react";

export default function EkgDownloadsPage() {
  document.title = "Ontology Lookup Service (OLS)";
  return (
    <Fragment>
      <EkgHeader section="downloads" />
      <main className="container mx-auto px-4 my-8">
        <div className="text-2xl font-bold my-6">
          Downloading Knowledge Graph Exports
        </div>
        <div>
          <p className="px-1 mb-2 text-justify">
            Neo4j and Solr databases exports of the KG used for this website can be downloaded from&thinsp;
            <a
              className="link-default"
              href="https://ftp.ebi.ac.uk/pub/databases/spot/kg/"
              rel="noopener noreferrer"
              target="_blank"
            >
            https://ftp.ebi.ac.uk/pub/databases/spot/kg/
            </a>.
          </p>
          {/* <DataTable columns={columns} data={data} /> */}
        </div>
      </main>
    </Fragment>
  );
}
