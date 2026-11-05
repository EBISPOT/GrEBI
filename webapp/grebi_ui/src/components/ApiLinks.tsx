import React from "react";
import { siteUrl } from "../app/instance";
import { Link } from "react-router-dom";
import urlJoin from "url-join";

export default function ApiLinks({
  apiUrl,
  betaApiUrl,
}: {
  apiUrl: string;
  betaApiUrl: string;
}) {
  return (
    <div className="flex gap-1" style={{ padding: 0 }}>
      <div>
        <Link to={apiUrl} target="_blank" rel="noopener noreferrer">
          <img
            src={urlJoin(siteUrl(), "/json.svg")}
            width={45}
            alt="JSON document"
          />
        </Link>
      </div>
      {/* <div>
        <Link to={betaApiUrl} target="_blank" rel="noopener noreferrer">
          <img
            src={urlJoin(siteUrl(), "/jsonbeta.svg")}
            width={45}
            alt="JSON beta document"
          />
        </Link>
      </div> */}
    </div>
  );
}
