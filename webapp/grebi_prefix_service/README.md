# GrEBI Prefix Service

A Rocket-based REST API for reprefixing CURIEs using GrEBI's prefix maps.

## Endpoint

### POST /reprefix

Reprefix a CURIE using the loaded prefix map.

**Request:**
```json
{
  "curie": "HGNC:123"
}
```

**Response:**
```json
{
  "result": "hgnc:123"
}
```

## Running

```bash
cargo run --release
```

## Environment Variables

- `GREBI_PREFIX_MAP_PATH`: Path to the prefix map JSON file (defaults to `../../dataload/prefix_maps/prefix_map_normalise.json`)
- `ROCKET_ADDRESS`: Server address (default: 127.0.0.1 in debug, 0.0.0.0 in release)
- `ROCKET_PORT`: Server port (default: 8080)

## Docker

```bash
docker build -t grebi_prefix_service -f webapp/grebi_prefix_service/Dockerfile .
docker run -p 8080:8080 grebi_prefix_service
```
