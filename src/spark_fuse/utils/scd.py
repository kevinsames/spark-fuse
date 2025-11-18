from __future__ import annotations

from enum import Enum
from typing import Iterable, Optional, Sequence, Union

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

__all__ = [
    "SCDMode",
    "apply_scd",
    "scd1_upsert",
    "scd2_upsert",
]


# Delimiter used for stable row-hash concatenation (Unit Separator)
UNIT_SEPARATOR = "\u241f"
SCD_SEQUENCE_COL = "__scd_seq"


class SCDMode(str, Enum):
    SCD1 = "SCD1"
    SCD2 = "SCD2"


def _is_delta_path(identifier: str) -> bool:
    """Heuristic: treat identifiers containing "/" or ":/" as a path; otherwise a table name."""
    return "/" in identifier or ":/" in identifier


def _read_target_df(spark: SparkSession, target: str) -> DataFrame:
    if _is_delta_path(target):
        return spark.read.format("delta").load(target)
    else:
        return spark.table(target)


def _delta_table(spark: SparkSession, target: str) -> DeltaTable:
    if _is_delta_path(target):
        return DeltaTable.forPath(spark, target)
    else:
        return DeltaTable.forName(spark, target)


def _write_append(df: DataFrame, target: str) -> None:
    if _is_delta_path(target):
        df.write.format("delta").mode("append").save(target)
    else:
        df.write.format("delta").mode("append").saveAsTable(target)


def _coalesce_cast_to_string(col: F.Column) -> F.Column:
    # Normalize None/Null -> empty string to ensure stable hashing; cast complex to string deterministically
    return F.coalesce(col.cast("string"), F.lit(""))


def _scd2_process_batch(
    spark: SparkSession,
    source_batch: DataFrame,
    target: str,
    *,
    business_keys: Sequence[str],
    tracked_columns: Sequence[str],
    effective_col: str,
    expiry_col: str,
    current_col: str,
    version_col: str,
    hash_col: str,
    ts_col: F.Column,
    cond_keys_sql: str,
    create_if_not_exists: bool,
    target_exists: bool,
) -> bool:
    """Apply SCD2 semantics for a batch that has at most one row per business key."""

    if not target_exists:
        if not create_if_not_exists:
            raise ValueError(f"Target '{target}' does not exist and create_if_not_exists=False")
        initial = (
            source_batch.withColumn(effective_col, ts_col)
            .withColumn(expiry_col, F.lit(None).cast("timestamp"))
            .withColumn(current_col, F.lit(True))
            .withColumn(version_col, F.lit(1).cast("bigint"))
        )
        _write_append(initial, target)
        return True

    target_dt = _delta_table(spark, target)
    target_cols = set(_read_target_df(spark, target).columns)

    if hash_col in target_cols:
        change_cond_sql = f"NOT (t.`{hash_col}` <=> s.`{hash_col}`)"
    else:
        change_cond_sql = (
            " OR ".join([f"NOT (t.`{c}` <=> s.`{c}`)" for c in tracked_columns]) or "false"
        )

    (
        target_dt.alias("t")
        .merge(
            source_batch.alias("s"),
            f"({cond_keys_sql}) AND t.`{current_col}` = true",
        )
        .whenMatchedUpdate(
            condition=F.expr(change_cond_sql),
            set={
                expiry_col: ts_col,
                current_col: F.lit(False),
            },
        )
        .execute()
    )

    tgt_current = (
        _read_target_df(spark, target)
        .where(F.col(current_col) == F.lit(True))
        .select(*business_keys)
    )

    s = source_batch.alias("s")
    tcur = tgt_current.alias("tcur")
    join_cond = [s[k] == tcur[k] for k in business_keys]
    joined = s.join(tcur, on=join_cond, how="left")
    is_new_or_changed = tcur[business_keys[0]].isNull()
    rows_to_insert = joined.where(is_new_or_changed).select([s[c] for c in source_batch.columns])

    tgt_max_ver = (
        _read_target_df(spark, target)
        .groupBy(*business_keys)
        .agg(F.max(F.col(version_col)).alias("__prev_version"))
    )

    rows_with_prev = rows_to_insert.join(tgt_max_ver, on=list(business_keys), how="left")

    to_insert = (
        rows_with_prev.withColumn(effective_col, ts_col)
        .withColumn(expiry_col, F.lit(None).cast("timestamp"))
        .withColumn(current_col, F.lit(True))
        .withColumn(
            version_col, F.coalesce(F.col("__prev_version"), F.lit(0)).cast("bigint") + F.lit(1)
        )
        .drop("__prev_version")
    )

    _write_append(to_insert, target)
    return True


def scd1_upsert(
    spark: SparkSession,
    source_df: DataFrame,
    target: str,
    *,
    business_keys: Sequence[str],
    tracked_columns: Optional[Iterable[str]] = None,
    dedupe_keys: Optional[Sequence[str]] = None,
    order_by: Optional[Sequence[str]] = None,
    hash_col: str = "row_hash",
    null_key_policy: str = "error",  # "error" | "drop"
    create_if_not_exists: bool = True,
) -> None:
    """Perform SCD Type 1 upsert (no history): keep exactly one current row per business key.

    Logic:
      - De-duplicate input on `dedupe_keys` (default business_keys) using `order_by` to keep latest.
      - Compute a stable `row_hash` over tracked columns.
      - MERGE into target: update when hash differs, insert when not matched.
    """

    if not business_keys:
        raise ValueError("business_keys must be a non-empty sequence")

    src_cols_set = set(source_df.columns)
    missing_keys = [k for k in business_keys if k not in src_cols_set]
    if missing_keys:
        raise ValueError(f"source_df missing business_keys: {missing_keys}")

    if tracked_columns is None:
        tracked_columns = [c for c in source_df.columns if c not in set(business_keys)]
    else:
        missing_tracked = [c for c in tracked_columns if c not in src_cols_set]
        if missing_tracked:
            raise ValueError(f"tracked_columns not in source_df: {missing_tracked}")

    # Null key policy
    key_cond = None
    for k in business_keys:
        key_cond = F.col(k).isNotNull() if key_cond is None else (key_cond & F.col(k).isNotNull())
    if null_key_policy == "drop":
        source_df = source_df.where(key_cond)
    elif null_key_policy == "error":
        null_cnt = source_df.where(~key_cond).limit(1).count()
        if null_cnt:
            raise ValueError(
                "Null business key encountered in source_df; set null_key_policy='drop' to drop them."
            )
    else:
        raise ValueError("null_key_policy must be 'error' or 'drop'")

    # De-dup
    if dedupe_keys is None:
        dedupe_keys = list(business_keys)

    if order_by and len(order_by) > 0:
        w = Window.partitionBy(*[F.col(k) for k in dedupe_keys]).orderBy(
            *[F.col(c).desc_nulls_last() for c in order_by]
        )
        source_df = (
            source_df.withColumn("__rn", F.row_number().over(w))
            .where(F.col("__rn") == 1)
            .drop("__rn")
        )
    else:
        source_df = source_df.dropDuplicates(list(dedupe_keys))

    # Hash tracked columns
    hash_expr_inputs = [_coalesce_cast_to_string(F.col(c)) for c in tracked_columns]
    row_hash_expr = F.sha2(F.concat_ws(UNIT_SEPARATOR, *hash_expr_inputs), 256)
    src_hashed = source_df.withColumn(hash_col, row_hash_expr)

    # Create target if missing
    target_exists = True
    try:
        _delta_table(spark, target)
    except Exception:
        target_exists = False

    if not target_exists:
        if not create_if_not_exists:
            raise ValueError(f"Target '{target}' does not exist and create_if_not_exists=False")
        _write_append(src_hashed, target)
        return

    # MERGE: update when changed, insert when new
    dt = _delta_table(spark, target)

    cond_keys_sql = " AND ".join([f"t.`{k}` <=> s.`{k}`" for k in business_keys])

    target_cols = set(_read_target_df(spark, target).columns)
    # Ensure target has hash column (add ephemeral compare if absent)
    if hash_col in target_cols:
        change_cond_sql = f"NOT (t.`{hash_col}` <=> s.`{hash_col}`)"
    else:
        change_cond_sql = (
            " OR ".join([f"NOT (t.`{c}` <=> s.`{c}`)" for c in tracked_columns]) or "false"
        )

    # Build column maps (exclude technical SCD2 columns if they exist in target)
    src_cols = src_hashed.columns
    # If target has SCD2 fields, do not attempt to write them in SCD1
    scd2_fields = {"effective_start_ts", "effective_end_ts", "is_current", "version"}
    write_cols = [c for c in src_cols if c not in scd2_fields]

    set_map = {c: F.col(f"s.`{c}`") for c in write_cols}
    insert_map = {c: F.col(f"s.`{c}`") for c in write_cols}

    (
        dt.alias("t")
        .merge(
            src_hashed.alias("s"),
            cond_keys_sql,
        )
        .whenMatchedUpdate(
            condition=F.expr(change_cond_sql),
            set=set_map,
        )
        .whenNotMatchedInsert(values=insert_map)
        .execute()
    )


def scd2_upsert(
    spark: SparkSession,
    source_df: DataFrame,
    target: str,
    *,
    business_keys: Sequence[str],
    tracked_columns: Optional[Iterable[str]] = None,
    dedupe_keys: Optional[Sequence[str]] = None,
    order_by: Optional[Sequence[str]] = None,
    effective_col: str = "effective_start_ts",
    expiry_col: str = "effective_end_ts",
    current_col: str = "is_current",
    version_col: str = "version",
    hash_col: str = "row_hash",
    load_ts_expr: Optional[Union[str, F.Column]] = None,
    null_key_policy: str = "error",  # "error" | "drop"
    create_if_not_exists: bool = True,
) -> None:
    """Perform SCD Type 2 upsert into a Delta table (Unity Catalog table name or Delta path).

    Two-step algorithm:
      1) Close currently active target rows when incoming record for same key has a different row hash.
      2) Insert new active rows for new keys or changed keys with incremented version.

    Args:
        spark: SparkSession used to read/write Delta tables.
        source_df: DataFrame containing incoming records. Can include duplicates; these are
            de-duplicated by `dedupe_keys`, keeping the latest row by `order_by`.
        target: Unity Catalog table name (for example, ``catalog.schema.table``) or Delta path
            (for example, ``dbfs:/path``).
        business_keys: Columns that uniquely identify an entity (merge condition).
        tracked_columns: Columns whose changes trigger a new version. Defaults to all non-key,
            non-metadata columns.
        dedupe_keys: Columns used to de-duplicate input before merge. Defaults to ``business_keys``.
        order_by: Columns used to choose the most recent record per ``dedupe_keys``. Highest values
            win.
        effective_col: Name of the effective-start timestamp column in the target dataset.
        expiry_col: Name of the effective-end timestamp column in the target dataset.
        current_col: Name of the boolean "is current" column in the target dataset.
        version_col: Name of the version column in the target dataset.
        hash_col: Name of the hash column used to detect row changes.
        load_ts_expr: PySpark Column or SQL expression string that provides the effective-start
            Timestamp to use for `effective_start_ts`. Accepts a PySpark Column or a SQL
            expression string (e.g., "current_timestamp()" or "to_timestamp('2020-01-01 00:00:00')").
            Defaults to `current_timestamp()`.
        null_key_policy: Policy for null business keys in ``source_df``. Either ``"error"`` (default)
            or ``"drop"``.
        create_if_not_exists: When ``True`` (default), create the target table if it does not exist.
    """

    if not business_keys:
        raise ValueError("business_keys must be a non-empty sequence")

    # Normalize/validate columns
    src_cols_set = set(source_df.columns)

    missing_keys = [k for k in business_keys if k not in src_cols_set]
    if missing_keys:
        raise ValueError(f"source_df missing business_keys: {missing_keys}")

    scd_meta = {effective_col, expiry_col, current_col, version_col, hash_col}
    if tracked_columns is None:
        tracked_columns = [c for c in source_df.columns if c not in set(business_keys) | scd_meta]
    else:
        missing_tracked = [c for c in tracked_columns if c not in src_cols_set]
        if missing_tracked:
            raise ValueError(f"tracked_columns not in source_df: {missing_tracked}")

    # Drop or error on null keys in source
    key_cond = None
    for k in business_keys:
        key_cond = F.col(k).isNotNull() if key_cond is None else (key_cond & F.col(k).isNotNull())
    if null_key_policy == "drop":
        source_df = source_df.where(key_cond)
    elif null_key_policy == "error":
        null_cnt = source_df.where(~key_cond).limit(1).count()
        if null_cnt:
            raise ValueError(
                "Null business key encountered in source_df; set null_key_policy='drop' to drop them."
            )
    else:
        raise ValueError("null_key_policy must be 'error' or 'drop'")

    # Partition incoming data by dedupe_keys so we can process one row per key in each pass.
    if dedupe_keys is None:
        dedupe_keys = list(business_keys)

    if order_by:
        w = Window.partitionBy(*[F.col(k) for k in dedupe_keys]).orderBy(
            *[F.col(c).desc_nulls_last() for c in order_by]
        )
        source_df = source_df.withColumn(SCD_SEQUENCE_COL, F.row_number().over(w))
    else:
        source_df = source_df.dropDuplicates(list(dedupe_keys)).withColumn(
            SCD_SEQUENCE_COL, F.lit(1)
        )

    # Compute deterministic row hash over tracked columns
    hash_expr_inputs = [_coalesce_cast_to_string(F.col(c)) for c in tracked_columns]
    row_hash_expr = F.sha2(
        F.concat_ws(UNIT_SEPARATOR, *hash_expr_inputs), 256
    )  # unit separator as delimiter
    source_hashed = source_df.withColumn(hash_col, row_hash_expr)

    # Accept either a Column or a SQL string for load timestamp
    if load_ts_expr is None:
        ts_col = F.current_timestamp()
    elif isinstance(load_ts_expr, str):
        ts_col = F.expr(load_ts_expr)
    else:
        ts_col = load_ts_expr

    # Determine merge condition and columns used for writing.
    cond_keys_sql = " AND ".join([f"t.`{k}` <=> s.`{k}`" for k in business_keys])
    # Does target exist?
    target_exists = True
    try:
        _delta_table(spark, target)
    except Exception:
        target_exists = False

    # Split into per-rank batches (rank 1 == latest). Process from oldest -> newest.
    should_cache = bool(order_by)
    if should_cache:
        source_hashed = source_hashed.cache()

    max_seq_val = source_hashed.agg(F.max(F.col(SCD_SEQUENCE_COL)).alias("__max_seq")).collect()[0][
        "__max_seq"
    ]

    if max_seq_val is None:
        if should_cache:
            source_hashed.unpersist()
        return

    create_flag = create_if_not_exists
    for seq in range(int(max_seq_val), 0, -1):
        batch = source_hashed.where(F.col(SCD_SEQUENCE_COL) == seq).drop(SCD_SEQUENCE_COL)
        target_exists = _scd2_process_batch(
            spark,
            batch,
            target,
            business_keys=business_keys,
            tracked_columns=tracked_columns,
            effective_col=effective_col,
            expiry_col=expiry_col,
            current_col=current_col,
            version_col=version_col,
            hash_col=hash_col,
            ts_col=ts_col,
            cond_keys_sql=cond_keys_sql,
            create_if_not_exists=create_flag,
            target_exists=target_exists,
        )
        create_flag = False

    if should_cache:
        source_hashed.unpersist()


def apply_scd(
    spark: SparkSession,
    source_df: DataFrame,
    target: str,
    *,
    scd_mode: SCDMode,
    **kwargs,
) -> None:
    """Unified entry point to apply SCD semantics.

    Examples
    --------
    apply_scd(spark, df, "main.dim.customer", scd_mode=SCDMode.SCD2, business_keys=["customer_id"], ...)
    """
    if scd_mode == SCDMode.SCD1:
        return scd1_upsert(spark, source_df, target, **kwargs)
    elif scd_mode == SCDMode.SCD2:
        return scd2_upsert(spark, source_df, target, **kwargs)
    else:
        raise ValueError(f"Unsupported scd_mode: {scd_mode}")
