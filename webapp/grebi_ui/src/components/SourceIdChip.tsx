import React from "react";
import { OpenInNew } from "@mui/icons-material";
import { copyToClipboard } from "../app/util";
import { externalLinkForId, dbIconUrl, ExternalLink } from "../db_links/dbLinks";

/** The icon of the database a link goes to, or a generic open-in-new glyph. */
export function DbIcon({ link, size = 14 }: { link: ExternalLink; size?: number }) {
  const style = { display: "inline-block", verticalAlign: "-2px", marginRight: 4 } as const;
  if (link.icon) {
    return <img src={dbIconUrl(link.icon)} alt="" width={size} height={size} style={style} />;
  }
  return <OpenInNew style={{ ...style, fontSize: size - 1 }} className="text-neutral-default" />;
}

/**
 * A source id as a chip. An id of a known kind links out to the database it
 * comes from, with that database's icon; every chip has a copy button.
 */
export default function SourceIdChip({ id }: { id: string }) {
  const link = externalLinkForId(id);
  return (
    <div className="bg-grey-default rounded-sm font-mono pl-1" style={{ fontSize: "small" }}>
      {link ? (
        <a href={link.url} target="_blank" rel="noopener noreferrer" title={`Open in ${link.database}`} className="hover:underline">
          <DbIcon link={link} />
          {id}
        </a>
      ) : (
        id
      )}{" "}
      <button onClick={() => { copyToClipboard(id); }} title="Copy">
        <i className="icon icon-common icon-copy icon-spacer" />
      </button>
    </div>
  );
}
