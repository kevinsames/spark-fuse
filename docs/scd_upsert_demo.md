# SCD Upsert Demo

The slowly changing dimension helpers (`scd1_upsert`, `scd2_upsert`, `apply_scd`) coordinate Delta MERGE operations to keep dimension tables current while preserving history when desired.

```python
from pyspark.sql import Row
from spark_fuse.spark import create_session
from spark_fuse.utils.scd import scd1_upsert, scd2_upsert, SCDMode, apply_scd

spark = create_session(app_name="spark-fuse-scd-example")
```

## SCD Type 1

SCD1 keeps a single current record per business key, overwriting prior values when changes arrive.

```python
initial = spark.createDataFrame(
    [Row(id=1, val="a", ts=1), Row(id=1, val="b", ts=2), Row(id=2, val="x", ts=5)]
)

target_path = "/tmp/scd1_demo"
scd1_upsert(
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
scd1_upsert(
    spark,
    updates,
    target_path,
    business_keys=["id"],
    tracked_columns=["val"],
    order_by=["ts"],
)
```

## SCD Type 2

SCD2 tracks history by closing current rows (setting expiry timestamp, `is_current=false`) and inserting new versions.

```python
target_path = "/tmp/scd2_demo"

source = spark.createDataFrame(
    [Row(id=1, val="a", ts=1), Row(id=1, val="b", ts=2), Row(id=2, val="x", ts=5)]
)

scd2_upsert(
    spark,
    source,
    target_path,
    business_keys=["id"],
    tracked_columns=["val"],
    order_by=["ts"],
    load_ts_expr="to_timestamp('2024-01-01 00:00:00')",
)

changes = spark.createDataFrame([Row(id=1, val="c", ts=3), Row(id=3, val="z", ts=1)])
scd2_upsert(
    spark,
    changes,
    target_path,
    business_keys=["id"],
    tracked_columns=["val"],
    order_by=["ts"],
    load_ts_expr="to_timestamp('2024-01-02 00:00:00')",
)
```

## Unified dispatcher

Use `apply_scd` to switch between SCD1 and SCD2 using a shared interface.

```python
dispatcher_target = "/tmp/apply_scd_demo"
apply_scd(
    spark,
    spark.createDataFrame([Row(id=1, val="a"), Row(id=2, val="b")]),
    dispatcher_target,
    scd_mode=SCDMode.SCD2,
    business_keys=["id"],
    tracked_columns=["val"],
    load_ts_expr="current_timestamp()",
)
```

## Notebook walkthrough

- [SCD Upsert Demo](https://github.com/kevinsames/spark-fuse/blob/main/notebooks/scd_demo.ipynb)
