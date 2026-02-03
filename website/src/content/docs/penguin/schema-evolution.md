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

## Type Widening

Penguin automatically allows safe type widening operations that don't lose data:

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

These conversions happen transparently without requiring a schema evolution commit.

## How It Works

When Penguin processes incoming files:

1. **Schema inference**: Infer schema from the first Parquet file
2. **Comparison**: Compare incoming schema against table schema
3. **Validation**: Check if changes are allowed by the configured mode
4. **Evolution**: Apply schema changes if needed (merge/overwrite modes)
5. **Commit**: Commit files with the updated schema

```
┌─────────────────────────────────────────────────────────────────┐
│                    Schema Evolution Flow                         │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌───────────────────┐                                           │
│  │  Infer Schema     │  Read schema from incoming Parquet file   │
│  └───────────────────┘                                           │
│           │                                                      │
│           ▼                                                      │
│  ┌───────────────────┐                                           │
│  │  Compare Schemas  │  Detect new/missing fields, type changes  │
│  └───────────────────┘                                           │
│           │                                                      │
│           ▼                                                      │
│  ┌───────────────────┐                                           │
│  │  Validate Mode    │  Check if changes allowed by config       │
│  └───────────────────┘                                           │
│           │                                                      │
│     ┌─────┴─────┐                                                │
│     ▼           ▼                                                │
│  ┌──────┐  ┌────────┐                                            │
│  │ Pass │  │  Fail  │  Reject commit with error                  │
│  └──────┘  └────────┘                                            │
│     │                                                            │
│     ▼                                                            │
│  ┌───────────────────┐                                           │
│  │  Apply Evolution  │  Update table metadata if needed          │
│  └───────────────────┘                                           │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
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
