import { useEffect, useRef } from "react";

export function asArray<T>(obj: T | T[]): T[] {
  if (Array.isArray(obj)) {
    return obj;
  } else if (obj) {
    return [obj];
  }
  return [];
}

export function randomString() {
  return (Math.random() * Math.pow(2, 54)).toString(36);
}

export function sortByKeys(a: any, b: any) {
  const keyA = a.key.toUpperCase();
  const keyB = b.key.toUpperCase();
  return keyA === keyB ? 0 : keyA > keyB ? 1 : -1;
}

export async function copyToClipboard(text: string) {
  if ("clipboard" in navigator) {
    return await navigator.clipboard.writeText(text);
  } else {
    return document.execCommand("copy", true, text);
  }
}

export function usePrevious(value: any) {
  const ref = useRef();
  useEffect(() => {
    ref.current = value;
  }, [value]);
  return ref.current;
}

export function mapToApiParams(searchParams: URLSearchParams) {
  const searchParamsCopy = new URLSearchParams();
  searchParams.forEach((value: string, key: string) => {
    let newKey = key.includes("_") ? toCamel(key) : key;
    // special cases
    if (newKey === "oboId") newKey = "curie";
    searchParamsCopy.append(newKey, value);
  });
  return searchParamsCopy;
}

export function toCamel(str: string) {
  return str.replace(/([-_][a-z])/gi, ($1) => {
    return $1.toUpperCase().replace("-", "").replace("_", "");
  });
}
