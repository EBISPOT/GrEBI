# `grebi_nodes2edges`

Re-shapes JSON objects which represent edges into something GrEBI will recognise as edges. For each input JSONL record, it extracts the `--from_field` and `--to_field` values as the source and destination of the edge, and carries everything else as edge metadata.

If the from or to fields contain arrays, the Cartesian product of all (from × to) combinations is produced.

## Usage

```
cat input.jsonl | grebi_nodes2edges --from_field <FIELD> --to_field <FIELD> --edge_type <TYPE> > output.jsonl
```

## Options

All three options are required.

| Option | Description |
|---|---|
| `--from_field <FIELD>` | The field to use as the edge source (`id` in the output). |
| `--to_field <FIELD>` | The field to use as the edge target (`grebi:value` in the output). |
| `--edge_type <TYPE>` | The edge type (property name in the output) |

## Behaviour

1. Each input record is parsed as JSON.
2. The from and to fields are extracted. If either is an array, all combinations are generated.
3. For each (from, to) pair, an output record is written with:
   - `id` set to the from value
   - A key named after `--edge_type` containing `grebi:value` (the to value) and `grebi:properties` (all remaining fields from the input).
