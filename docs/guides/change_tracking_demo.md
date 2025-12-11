# Change Tracking Demo

The change-tracking helpers (`current_only_upsert`, `track_history_upsert`, `apply_change_tracking`) coordinate Delta MERGE operations so you can either keep a single current record per business key or maintain the full history of changes. They also plug into `DataFrame.write.change_tracking` for fluent option parsing.

```python
from pyspark.sql import Row
from spark_fuse.spark import create_session
from spark_fuse.utils import change_tracking
from spark_fuse.utils.change_tracking import (
    ChangeTrackingMode,
    apply_change_tracking,
    current_only_upsert,
    track_history_upsert,
)

spark = create_session(app_name="spark-fuse-change-tracking-demo")
```

## Current-only merges

`current_only_upsert` keeps a single current row per business key, overwriting previous values when a change arrives.

```python
initial = spark.createDataFrame(
    [Row(id=1, val="a", ts=1), Row(id=1, val="b", ts=2), Row(id=2, val="x", ts=5)]
)

target_path = "/tmp/current_only_demo"
current_only_upsert(
    spark,
    initial,
    target_path,
    business_keys=["id"],
    tracked_columns=["val"],
    order_by=["ts"],
)

updates = spark.createDataFrame(
    [Row(id=1, val="c", ts=3), Row(id=3, val="z", ts=1)]
)
current_only_upsert(
    spark,
    updates,
    target_path,
    business_keys=["id"],
    tracked_columns=["val"],
    order_by=["ts"],
)
```

If future batches introduce new attributes, enable Delta schema evolution:

```python
current_only_upsert(
    spark,
    spark.createDataFrame([Row(id=1, val="d", color="blue", ts=4)]),
    target_path,
    business_keys=["id"],
    tracked_columns=["val", "color"],
    order_by=["ts"],
    allow_schema_evolution=True,
)
```

## Track history

`track_history_upsert` closes the active row for a key (sets `is_current=false` and writes the expiry timestamp) before inserting a new version. When a batch contains multiple rows for the same key, the helper processes them chronologically so every intermediate change is preserved.

```python
history_target = "/tmp/track_history_demo"

source = spark.createDataFrame(
    [Row(id=1, val="a", ts=1), Row(id=1, val="b", ts=2), Row(id=2, val="x", ts=5)]
)

track_history_upsert(
    spark,
    source,
    history_target,
    business_keys=["id"],
    tracked_columns=["val"],
    order_by=["ts"],
    load_ts_expr="to_timestamp('2024-01-01 00:00:00')",
)

changes = spark.createDataFrame([Row(id=1, val="c", ts=3), Row(id=3, val="z", ts=1)])
track_history_upsert(
    spark,
    changes,
    history_target,
    business_keys=["id"],
    tracked_columns=["val"],
    order_by=["ts"],
    load_ts_expr="to_timestamp('2024-01-02 00:00:00')",
)
```

When new attributes appear later, schema evolution keeps the Delta table aligned:

```python
track_history_upsert(
    spark,
    spark.createDataFrame([Row(id=1, val="d", color="blue", ts=4)]),
    history_target,
    business_keys=["id"],
    tracked_columns=["val", "color"],
    order_by=["ts"],
    load_ts_expr="current_timestamp()",
    allow_schema_evolution=True,
)
```

## Unified dispatcher and writer sugar

Use `apply_change_tracking` to switch between strategies with a shared signature:

```python
dispatcher_target = "/tmp/apply_change_tracking_demo"
apply_change_tracking(
    spark,
    spark.createDataFrame([Row(id=1, val="a"), Row(id=2, val="b")]),
    dispatcher_target,
    change_tracking_mode=ChangeTrackingMode.TRACK_HISTORY,
    business_keys=["id"],
    tracked_columns=["val"],
    load_ts_expr="current_timestamp()",
)
```

For an even more fluent API, call `df.write.change_tracking`:

```python
spark.range(2).toDF("id").write.change_tracking.options(
    change_tracking_mode="track_history",
    change_tracking_options={
        "business_keys": ["id"],
        "tracked_columns": ["id"],
        "load_ts_expr": "current_timestamp()",
    },
).table("catalog.schema.dim_id")
```

## Notebook walkthrough

- [Change Tracking Demo](https://github.com/kevinsames/spark-fuse/blob/main/notebooks/demos/change_tracking_demo.ipynb)
