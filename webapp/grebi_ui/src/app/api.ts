
type ReqParams = {[k:string]:(string|string[])}|undefined 

/**
 * A request the API answered with an error status. The message is the API's
 * own ({"error": "..."}) when it gave one, else the status line.
 */
export class ApiError extends Error {
  status: number
  url: string
  constructor(status: number, url: string, message: string) {
    super(message)
    this.name = 'ApiError'
    this.status = status
    this.url = url
  }
}

/** What went wrong, for people. */
export function describeError(error: any): string {
  if (error instanceof ApiError) {
    return error.status === 404 ? (error.message || 'Not found') : `${error.message || 'The API returned an error'} (HTTP ${error.status})`
  }
  if (error instanceof TypeError) {
    // what fetch throws when the API cannot be reached at all
    return 'The API could not be reached'
  }
  return (error && error.message) || String(error)
}

function buildSearchParams(reqParams:ReqParams|URLSearchParams):string {
  if( reqParams instanceof URLSearchParams) {
    return reqParams.toString()
  }
  let params = new URLSearchParams()
  for(let key in reqParams) {
    let val = reqParams[key]
    if(Array.isArray(val)) {
      for(let v of val) {
        params.append(key, v)
      }
    } else {
      params.append(key, val)
    }
  }
  return params.toString()
}

export async function request(
  path: string,
  reqParams:ReqParams|URLSearchParams,
  init?: RequestInit | undefined,
  apiUrl?: string
): Promise<any> {
  const url = (apiUrl || process.env.REACT_APP_APIURL) + path;
  const message = `Loading ${url}`
  console.log(message)
  console.time(message)
  //const res = await fetch(url.replace(/([^:]\/)\/+/g, "$1"), {
  const res = await fetch(url + (reqParams ? ('?' + buildSearchParams(reqParams)) : ''), {
    ...(init ? init : {}),
    //headers: { ...(init?.headers || {}), ...getAuthHeaders() }
  });
  console.timeEnd(message)
  if (!res.ok) {
    console.dir(`Failure loading ${res.url} with status ${res.status} (${res.statusText})`);
    let detail = `${res.status} ${res.statusText}`.trim()
    try {
      const body = await res.json()
      if (body && typeof body.error === 'string') {
        detail = body.error
      } else if (body && typeof body.message === 'string') {
        detail = body.message
      }
    } catch (e) {
      // no JSON body: the status line will do
    }
    return Promise.reject(new ApiError(res.status, res.url, detail))
  }
  return await res.json();
}

export class Page<T> {
  constructor(
    public page: number,
    public numElements: number,
    public totalPages: number,
    public totalElements: number,
    public elements: T[],
    public facetFieldsToCounts: Map<string, Map<string, number>>
  ) {}

  map<NewType>(fn: (T) => NewType) {
    return new Page<NewType>(
      this.page,
      this.numElements,
      this.totalPages,
      this.totalElements,
      this.elements.map((el) => {
        let res = fn(el);
        if(!res) {
          throw new Error("Page.map function returned null or undefined")
        }
        return res;
      }),
      this.facetFieldsToCounts
    );
  }
}

export async function getPaginated<ResType>(
  path: string,
  reqParams?: ReqParams|URLSearchParams,
  apiUrl?: string
): Promise<Page<ResType>> {
  const res = await get<any>(path, reqParams, apiUrl);

  return new Page<ResType>(
	res.number || 0,
	res.numberOfElements || 0,
	res.totalPages || 0,
	(res.totalElements ?? res.total) || 0,
	res.content || [],
	res.facetFieldToCounts || new Map()
  );
}

export async function get<ResType>(path: string, reqParams?:ReqParams|URLSearchParams, apiUrl?: string): Promise<ResType> {
  return request(path, reqParams, undefined, apiUrl);
}

export async function post<ReqType, ResType = any>(
  path: string,
  reqParams: ReqParams,
  body: ReqType
): Promise<ResType> {
  return request(path, reqParams, {
    method: "POST",
    body: JSON.stringify(body),
    headers: {
      "content-type": "application/json",
    },
  });
}

export async function put<ReqType, ResType = any>(
  path: string,
  reqParams: ReqParams,
  body: ReqType
): Promise<ResType> {
  return request(path, reqParams, {
    method: "PUT",
    body: JSON.stringify(body),
    headers: {
      "content-type": "application/json",
    },
  });
}
