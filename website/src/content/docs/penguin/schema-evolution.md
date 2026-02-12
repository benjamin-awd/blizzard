---
title: Schema Evolution
description: How Penguin handles schema changes in incoming Parquet files
---

Penguin supports automatic schema evolution, allowing graceful handling of schema changes in incoming Parquet files. This is essential for real-world data pipelines where source schemas evolve over time.

## Evolution Modes

Penguin provides three schema evolution modes, configured via the `schema_evolution` option:

| Mode | Description |
|------|-------------|
| `strict` | Reject any schema changes |
| `merge` | Allow adding new nullable columns (default) |
| `overwrite` | Replace schema entirely (requires explicit opt-in) |

### Merge Mode (Default)

Merge mode is the safe default that allows your schema to evolve without breaking existing data:

```yaml
tables:
  events:
    table_uri: "s3://my-bucket/delta-tables/events"
    schema_evolution: merge  # This is the default
```

In merge mode:
- **New nullable fields**: Allowed (added with NULL default for existing rows)
- **New required fields**: Rejected (would break existing data)
- **Missing fields**: Allowed (filled with NULL on read)
- **Type widening**: Allowed (e.g., Int32 to Int64)
- **Utf8 compatibility**: Scalar types accepted when table column is Utf8 (e.g., Float64 in Parquet with Utf8 in table)
- **Type narrowing**: Rejected (could lose data)
- **Incompatible type changes**: Rejected (e.g., Int64 to String)

### Strict Mode

Strict mode rejects any schema changes, useful when you need to guarantee schema stability:

```yaml
tables:
  events:
    table_uri: "s3://my-bucket/delta-tables/events"
    schema_evolution: strict
```

Use this when:
- You have strict data contracts
- Schema changes require manual review and approval
- Downstream consumers can't handle schema changes

### Overwrite Mode

Overwrite mode replaces the schema entirely with the incoming schema:

```yaml
tables:
  events:
    table_uri: "s3://my-bucket/delta-tables/events"
    schema_evolution: overwrite  # Use with caution!
```

:::caution
Overwrite mode can break existing data and downstream consumers. Only use this when you understand the implications and have a recovery plan.
:::

## Type Compatibility

Penguin automatically allows certain type differences between the table schema and incoming Parquet files without requiring a schema evolution commit.

### Type Widening

Safe numeric widening operations that don't lose data:

| From | To |
|------|-----|
| `Int8` | `Int16`, `Int32`, `Int64` |
| `Int16` | `Int32`, `Int64` |
| `Int32` | `Int64` |
| `UInt8` | `UInt16`, `UInt32`, `UInt64` |
| `UInt16` | `UInt32`, `UInt64` |
| `UInt32` | `UInt64` |
| `Float32` | `Float64` |
| `Date32` | `Date64` |

### Utf8 Compatibility

When the table schema has a `Utf8` (or `LargeUtf8`) column, Penguin accepts incoming Parquet files where that column has any scalar type (e.g., `Float64`, `Int64`, `Boolean`). The table schema stays as `Utf8` and readers cast the values at read time.

This is particularly useful when using Blizzard's `coerce_conflicts_to_utf8` inference mode. Schema inference may produce different types across restarts depending on the data sampled — for example, a field might be inferred as `Utf8` when the sample contains mixed types (objects and strings), but as `Float64` when the sample only contains numbers. Utf8 compatibility ensures these Parquet files are accepted without error.

Composite types (`Struct`, `List`, `Map`) are **not** compatible with `Utf8` columns and will still be rejected.

| Table Type | Incoming Type | Compatible? |
|------------|---------------|-------------|
| `Utf8` | `Float64` | Yes |
| `Utf8` | `Int64` | Yes |
| `Utf8` | `Boolean` | Yes |
| `Utf8` | `Struct(...)` | No |
| `Utf8` | `List(...)` | No |
| `Float64` | `Utf8` | No (reverse direction not allowed) |

## How It Works

When Penguin processes incoming files:

1. **Schema inference**: Infer schema from the first Parquet file
2. **Comparison**: Compare incoming schema against table schema
3. **Validation**: Check if changes are allowed by the configured mode
4. **Evolution**: Apply schema changes if needed (merge/overwrite modes)
5. **Commit**: Commit files with the updated schema

```d2
direction: down
Schema Evolution Flow: {
  infer: Infer Schema {
    label: "Infer Schema\nRead schema from incoming Parquet file"
  }
  compare: Compare Schemas {
    label: "Compare Schemas\nDetect new/missing fields, type changes"
  }
  validate: Validate Mode {
    label: "Validate Mode\nCheck if changes allowed by config"
  }
  pass: Pass {
    style.fill: "#ccffcc"
  }
  fail: Fail {
    label: "Fail\nReject commit with error"
    style.fill: "#ffcccc"
  }
  apply: Apply Evolution {
    label: "Apply Evolution\nUpdate table metadata if needed"
  }

  infer -> compare -> validate
  validate -> pass
  validate -> fail
  pass -> apply
}
```

## Examples

### Adding a New Column

Suppose your source data adds a new `email` field:

**Before:**
```json
{"id": 1, "name": "Alice"}
```

**After:**
```json
{"id": 1, "name": "Alice", "email": "alice@example.com"}
```

In **merge mode**, Penguin will:
1. Detect the new `email` field
2. Add it to the table schema as nullable
3. Existing rows will have `NULL` for the `email` column

### Type Change (Widening)

If your source changes from `Int32` to `Int64`:

```json
{"id": 1, "count": 2147483648}  // Exceeds Int32 max
```

In **merge mode**, Penguin will:
1. Detect the type change from `Int32` to `Int64`
2. Allow the change (type widening is safe)
3. Continue processing without schema modification

### Utf8 Column with Scalar Parquet Type

When using Blizzard's `coerce_conflicts_to_utf8` inference, a field like `max_threshold` may be inferred as `Utf8` because the first file contained mixed types (e.g., both `"none"` and `0.5`). The Delta table is created with `Utf8` for that column.

Later, a new file arrives where `max_threshold` only contains numeric values. Blizzard infers it as `Float64` and writes a Parquet file with a `Float64` column:

```
Table schema:    max_threshold: Utf8
Parquet file:    max_threshold: Float64
```

In **merge mode**, Penguin will:
1. Detect the type difference (`Utf8` → `Float64`)
2. Accept the file (scalar types are compatible with `Utf8` columns)
3. Keep the table schema as `Utf8` — no schema evolution commit needed
4. Readers cast the `Float64` Parquet values to strings at read time

### Incompatible Change

If your source changes a field type incompatibly:

**Before:**
```json
{"id": 1, "timestamp": 1706500000}  // Int64
```

**After:**
```json
{"id": 1, "timestamp": "2024-01-29T00:00:00Z"}  // String
```

In **merge mode**, Penguin will:
1. Detect the type change from `Int64` to `String`
2. Reject the commit with an error
3. The file remains uncommitted for manual resolution

## Error Handling

When schema evolution fails, Penguin logs detailed error messages:

```
ERROR Schema error: Type change not allowed for field 'timestamp': Int64 -> Utf8
```

```
ERROR Schema error: Cannot add required field 'user_id' - new fields must be nullable
```

```
ERROR Schema error: Incompatible schema: new required fields: ["required_field"]
```

The uncommitted files remain in the table directory, allowing you to:
1. Fix the upstream schema issue
2. Manually handle the files
3. Change the evolution mode if appropriate

## Best Practices

1. **Start with merge mode**: It provides a good balance between flexibility and safety
2. **Use strict mode for critical tables**: When downstream systems can't handle changes
3. **Monitor schema evolution**: Watch for unexpected new fields that might indicate data quality issues
4. **Test schema changes**: Before deploying source schema changes, test against a non-production environment
5. **Document your schemas**: Maintain schema documentation for data contracts
