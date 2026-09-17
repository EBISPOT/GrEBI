import React from "react";
import { ApiError, describeError } from "../app/api";

/** What failed and why, in place of the content that could not be loaded. */
export default function ErrorMessage({ error, what }: { error: any; what: string }) {
  const notFound = error instanceof ApiError && error.status === 404;
  return (
    <div role="alert" className="my-4 rounded-md border border-red-300 bg-red-50 px-4 py-3 text-red-900">
      <div className="font-bold">{notFound ? `${what} not found` : `${what} could not be loaded`}</div>
      <div className="text-sm mt-1">{describeError(error)}</div>
    </div>
  );
}
