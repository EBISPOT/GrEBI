
# `grebi_transform_jsonl`

A high-throughput general purpose JSONL transformer written in Rust. Reads JSONL from stdin, applies a series of transformations, and writes JSONL to stdout.

## Usage

```
cat input.jsonl | grebi_transform_jsonl [OPTIONS] > output.jsonl
```

## Options

All options are optional; no options = passthrough.

| Option | Description |
|---|---|
| `--json_select_keys <KEYS>` | Comma-separated list of keys to keep. All other keys are removed. |
| `--json_remove_keys <KEYS>` | Comma-separated list of keys to remove. |
| `--json_rename <FROM:TO>` | Rename a key. Supports nested paths with `.` (e.g. `foo.bar:baz`). Can be specified multiple times. |
| `--json_inject_type <TYPE>` | Inject a `grebi:type` field with the given value. |
| `--json_inject_key_prefix <PREFIX>` | Prefix all property keys (except `id` and keys already containing `:`) with this string. Defaults to empty. |
| `--json_inject_value_prefix <KEY:PREFIX>` | Prefix all values of the given key with the given string. Can be specified multiple times. |
| `--json_de_nest_field <FIELD.SUBFIELD>` | Extract a nested field from object values. The subfield becomes the `grebi:value` and remaining fields become `grebi:properties`. Can be specified multiple times. |
| `--json_select_by_value <KEY:VALUE>` | Only output records where the given key has the given string value. Can be specified multiple times; all conditions must match. |
| `--json_inject_hashid` | Flag. Add a `grebi:hashId` field containing a SHA-1 hash of the entire JSON object. |

## Behaviour

1. Each line of input is parsed as a JSON object.
2. If `--json_select_by_value` is specified, records not matching all conditions are discarded.
3. If `--json_inject_type` is specified, a `grebi:type` array is added.
4. Keys are filtered by `--json_select_keys` or `--json_remove_keys`.
5. Non-`id` keys without a `:` are prefixed with `--json_inject_key_prefix`.
6. Values are mapped: `--json_inject_value_prefix` prepends strings; `--json_de_nest_field` restructures nested objects.
7. If `--json_inject_hashid` is set, a SHA-1 hash is computed and stored.
8. Key renames from `--json_rename` are applied last.