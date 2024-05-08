
export default function encodeNodeId(id:string) {
    return btoa(id).replace(/=+$/, '')
}

