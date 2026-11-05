import React from "react";
import { useEmbedded } from "../app/embedded";

/** Covers the viewport while loading, or only its (positioned) container when scoped, as it always is inside a widget. */
export default function LoadingOverlay({ message, scoped }: { message?: string; scoped?: boolean }) {
  const embedded = useEmbedded();
  return (
    <div className={`${scoped || embedded ? "absolute" : "fixed"} top-0 left-0 w-full h-full z-50`}>
      <div className="absolute top-1/2 left-1/2 text-center bg-neutral-light/70 p-7 rounded-xl">
        <div className="spinner-default w-20 h-20"></div>
        <div className="text-neutral-black font-bold ">{message}</div>
      </div>
    </div>
  );
}
