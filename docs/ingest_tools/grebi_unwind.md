# `grebi_unwind`

Unwinds an array field into separate JSONL records. For each element of the specified array field, a copy of the entire record is emitted with that field replaced by the single element.

## Usage

```
cat input.jsonl | grebi_unwind --unwind_field <FIELD> > output.jsonl
```

## Options

| Option | Required | Description |
|---|---|---|
| `--unwind_field <FIELD>` | **Yes** | The array field to unwind. |

## Example

Given the input:

```json
{"id": "x", "tags": ["a", "b"]}
```

Running `grebi_unwind --unwind_field tags` produces:

```json
{"id": "x", "tags": "a"}
{"id": "x", "tags": "b"}
```
