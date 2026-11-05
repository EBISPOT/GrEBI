import { configureInstance } from "../app/instance";
import { defineGrebiWidgets, loadFonts } from "./elements";

/**
 * widgets.js: the script other websites include for the <grebi-…> elements
 * (the "Embedding widgets" page of the docs). The site is the one the script
 * was served from, and so is the API, unless the script tag's data-api
 * attribute names another.
 */
const script = document.currentScript as HTMLScriptElement | null;
const site = script && script.src ? new URL(".", script.src).href : "/";
const api = script && script.dataset.api ? script.dataset.api : new URL(process.env.REACT_APP_APIURL || "/", site).href;
configureInstance({ siteUrl: site, apiUrl: api.endsWith("/") ? api : api + "/" });
loadFonts(document);
defineGrebiWidgets();
