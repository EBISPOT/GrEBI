import { KeyboardArrowDown } from "@mui/icons-material";
import React from "react";

/**
 * The name of a language in the user's own language ("French" in an English
 * browser), falling back to the code. Languages are not countries, so no flags.
 */
export function languageName(lang: string): string {
  try {
    const locale = typeof navigator !== "undefined" && navigator.language ? navigator.language : "en";
    const name = new Intl.DisplayNames([locale], { type: "language" }).of(lang);
    return name || lang;
  } catch {
    return lang;
  }
}

export default function LanguagePicker({
  languages,
  lang,
  onChangeLang,
}: {
  languages:string[],
  lang: string;
  onChangeLang: (lang: string) => void;
}) {
  return (
    <div className="flex items-center group relative text-md">
      <select
        aria-label="Language"
        className="input-default appearance-none pr-7 z-20 bg-transparent cursor-pointer max-w-xs"
        onChange={(e) => {
          onChangeLang(e.target.value);
        }}
        value={lang}
      >
        {languages.map((code) => {
          return (
            <option key={code} value={code}>
              {languageName(code)}
            </option>
          );
        })}
      </select>
      <div className="absolute right-2 top-2 z-10 text-neutral-default group-focus:text-neutral-dark group-hover:text-neutral-dark">
        <KeyboardArrowDown fontSize="medium" />
      </div>
    </div>
  );
}
