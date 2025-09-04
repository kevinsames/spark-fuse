from __future__ import annotations

from typing import Iterable, Optional, Sequence

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


__all__ = ["scd2_upsert"]


def scd2_upsert(
    spark: SparkSession,
    source_df: DataFrame,
    target_path: str,
    *,
    business_keys: Sequence[str],
    effective_col: str = "effective_ts",
    expiry_col: str = "expiry_ts",
    current_col: str = "is_current",
    compare_columns: Optional[Iterable[str]] = None,
    timestamp_expr: Optional[F.Column] = None,
) -> None:
    """Upsert a DataFrame into a Delta table using a simple SCD Type 2 pattern.

    Strategy (two-phase):
    1) Expire current target rows that have changes vs. the incoming source rows.
    2) Insert new current rows for new or changed keys.

    Notes:
    - This is a pragmatic, minimal helper suitable for small/medium loads.
    - For very large datasets, consider optimizing joins and using partitioning/z-order.
    - `timestamp_expr` defaults to `current_timestamp()`.
    """

    ts_col = timestamp_expr or F.current_timestamp()

    scd_cols = {effective_col, expiry_col, current_col}
    if compare_columns is None:
        compare_columns = [c for c in source_df.columns if c not in set(business_keys) | scd_cols]

    # Try to detect if target exists; if not, bootstrap with initial insert.
    target_exists = True
    try:
        DeltaTable.forPath(spark, target_path)
    except Exception:
        target_exists = False

    if not target_exists:
        initial = (
            source_df
            .withColumn(effective_col, ts_col)
            .withColumn(expiry_col, F.lit(None).cast("timestamp"))
            .withColumn(current_col, F.lit(True))
        )
        initial.write.format("delta").mode("append").save(target_path)
        return

    # 1) Expire changed current rows via merge.
    target = DeltaTable.forPath(spark, target_path)
    cond_keys = " AND ".join([f"t.`{k}` <=> s.`{k}`" for k in business_keys])
    change_cond = " OR ".join([f"NOT (t.`{c}` <=> s.`{c}`)" for c in compare_columns]) or "false"

    (  # type: ignore[func-returns-value]
        target.alias("t")
        .merge(
            source_df.alias("s"),
            f"({cond_keys}) AND t.`{current_col}` = true",
        )
        .whenMatchedUpdate(
            condition=F.expr(change_cond),
            set={
                expiry_col: ts_col,
                current_col: F.lit(False),
            },
        )
        .execute()
    )

    # 2) Insert new current rows for new or changed keys.
    tgt_current = (
        spark.read.format("delta").load(target_path).where(F.col(current_col) == F.lit(True))
    )

    s = source_df.alias("s")
    t = tgt_current.alias("t")
    join_cond = [s[k] == t[k] for k in business_keys]
    joined = s.join(t, on=join_cond, how="left")

    # Determine change across payload columns using null-safe equality.
    diffs = [~F.expr(f"s.`{c}` <=> t.`{c}`") for c in compare_columns]
    any_changed = F.lit(False)
    for d in diffs:
        any_changed = any_changed | d

    is_new = t[business_keys[0]].isNull()
    rows_to_insert = joined.where(is_new | any_changed).select([s[c] for c in source_df.columns])

    to_insert = (
        rows_to_insert
        .withColumn(effective_col, ts_col)
        .withColumn(expiry_col, F.lit(None).cast("timestamp"))
        .withColumn(current_col, F.lit(True))
    )

    to_insert.write.format("delta").mode("append").save(target_path)

