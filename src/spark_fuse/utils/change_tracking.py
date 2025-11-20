from __future__ import annotations

from contextlib import contextmanager
from enum import Enum
from typing import Any, Dict, Iterable, Mapping, Optional, Sequence, Union

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.readwriter import DataFrameWriter

__all__ = [
    "ChangeTrackingMode",
    "apply_change_tracking",
    "apply_change_tracking_from_options",
    "current_only_upsert",
    "track_history_upsert",
    "change_tracking_writer",
    "enable_change_tracking_accessors",
    "ChangeTrackingWriteBuilder",
]


# Delimiter used for stable row-hash concatenation (Unit Separator)
UNIT_SEPARATOR = "\u241f"
CHANGE_TRACKING_SEQUENCE_COL = "__change_tracking_seq"


class ChangeTrackingMode(str, Enum):
    """Accepted values for ``change_tracking_mode``.

    * ``CURRENT_ONLY`` – keep exactly one current row per key (Type 1 semantics).
    * ``TRACK_HISTORY`` – create new versions + close previous ones (Type 2 semantics).
    """

    CURRENT_ONLY = "current_only"
    TRACK_HISTORY = "track_history"


_MODE_ALIASES = {
    "1": ChangeTrackingMode.CURRENT_ONLY,
    "current": ChangeTrackingMode.CURRENT_ONLY,
    "current_only": ChangeTrackingMode.CURRENT_ONLY,
    "currentonly": ChangeTrackingMode.CURRENT_ONLY,
    "2": ChangeTrackingMode.TRACK_HISTORY,
    "track_history": ChangeTrackingMode.TRACK_HISTORY,
    "trackhistory": ChangeTrackingMode.TRACK_HISTORY,
    "history": ChangeTrackingMode.TRACK_HISTORY,
}

_MODE_OPTION_KEY = "change_tracking_mode"
_COMMON_OPTION_KEYS = {"change_tracking_options"}
_MODE_SPECIFIC_OPTION_KEYS = {
    ChangeTrackingMode.CURRENT_ONLY: {"current_only_options"},
    ChangeTrackingMode.TRACK_HISTORY: {"track_history_options"},
}


def _normalize_option_key(key: Any) -> str:
    if key is None:
        raise ValueError("Option keys must be non-null")
    return str(key).strip().lower()


def _resolve_mode(value: Union[ChangeTrackingMode, str, int]) -> ChangeTrackingMode:
    if isinstance(value, ChangeTrackingMode):
        return value
    if isinstance(value, int):
        if value == 1:
            return ChangeTrackingMode.CURRENT_ONLY
        if value == 2:
            return ChangeTrackingMode.TRACK_HISTORY
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in _MODE_ALIASES:
            return _MODE_ALIASES[normalized]
    raise ValueError(
        f"Unsupported change_tracking_mode '{value}'. Use 1/2 or current_only/track_history."
    )


def _ensure_mapping(value: Any, *, option_name: str) -> Dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, Mapping):
        return dict(value)
    raise TypeError(
        f"Option '{option_name}' must be a mapping/dict of keyword arguments for the change tracking strategy."
    )


def _extract_tracking_kwargs_from_options(
    options: Mapping[str, Any],
) -> tuple[ChangeTrackingMode, Dict[str, Any]]:
    if not options:
        raise ValueError("At least the 'change_tracking_mode' option must be provided.")

    normalized: Dict[str, Any] = {}
    for key, value in options.items():
        normalized[_normalize_option_key(key)] = value

    if _MODE_OPTION_KEY not in normalized:
        raise ValueError("Missing required option 'change_tracking_mode'.")

    change_tracking_mode = _resolve_mode(normalized[_MODE_OPTION_KEY])

    option_keys = set(_COMMON_OPTION_KEYS) | _MODE_SPECIFIC_OPTION_KEYS[change_tracking_mode]
    change_tracking_kwargs: Dict[str, Any] = {}
    for alias in option_keys:
        if alias in normalized:
            change_tracking_kwargs = _ensure_mapping(normalized[alias], option_name=alias)
            break

    return change_tracking_mode, change_tracking_kwargs


class ChangeTrackingWriteBuilder:
    """Thin wrapper that applies change-tracking semantics via ``df.write.change_tracking``."""

    def __init__(self, df: DataFrame):
        self._df = df
        self._spark = df.sparkSession
        self._options: Dict[str, Any] = {}

    def option(self, key: str, value: Any) -> "ChangeTrackingWriteBuilder":
        self._options[str(key)] = value
        return self

    def options(self, *args: Mapping[str, Any], **kwargs: Any) -> "ChangeTrackingWriteBuilder":
        if len(args) > 1:
            raise TypeError("options() accepts at most one positional mapping argument.")
        if args:
            mapping = args[0]
            if not isinstance(mapping, Mapping):
                raise TypeError("options() positional argument must be a mapping/dict.")
            for key, value in mapping.items():
                self.option(key, value)
        for key, value in kwargs.items():
            self.option(key, value)
        return self

    def clear(self) -> "ChangeTrackingWriteBuilder":
        self._options.clear()
        return self

    def table(self, name: str, **options: Any) -> None:
        if options:
            self.options(**options)
        # Use a copy so that downstream mutations do not affect stored state.
        payload = dict(self._options)
        try:
            apply_change_tracking_from_options(
                spark=self._spark, source_df=self._df, target=name, options=payload
            )
        finally:
            self.clear()


def change_tracking_writer(df: DataFrame) -> ChangeTrackingWriteBuilder:
    """Return a builder that mirrors ``DataFrameWriter`` but routes to change-tracking helpers."""

    return ChangeTrackingWriteBuilder(df)


def enable_change_tracking_accessors(*, force: bool = False) -> None:
    """Attach ``.change_tracking`` helpers to ``DataFrame``/``DataFrameWriter`` for fluent usage.

    Once enabled (this module runs it on import), both ``df.change_tracking`` and
    ``df.write.change_tracking`` expose the :class:`ChangeTrackingWriteBuilder`, enabling fluent
    expressions such as ``df.write.change_tracking.options(...).table(...)``.

    Args:
        force: When ``True`` re-installs the accessors even if they already exist.
    """

    df_attrs = getattr(DataFrame, "__dict__", {})
    dfw_attrs = getattr(DataFrameWriter, "__dict__", {})

    def _df_change_tracking(self: DataFrame) -> ChangeTrackingWriteBuilder:
        return change_tracking_writer(self)

    def _dfw_change_tracking(self: DataFrameWriter) -> ChangeTrackingWriteBuilder:
        return change_tracking_writer(self._df)

    if force or "change_tracking" not in df_attrs:
        DataFrame.change_tracking = property(_df_change_tracking)  # type: ignore[attr-defined]
    if force or "change_tracking" not in dfw_attrs:
        DataFrameWriter.change_tracking = property(_dfw_change_tracking)  # type: ignore[attr-defined]


enable_change_tracking_accessors()


def apply_change_tracking_from_options(
    spark: SparkSession,
    source_df: DataFrame,
    target: str,
    *,
    options: Mapping[str, Any],
) -> None:
    """Route the write based on ``df.write``-style options.

    Used by :class:`ChangeTrackingWriteBuilder` and data-frame accessors.

    Expected keys:
      - ``change_tracking_mode``: accepts ``1``/``2`` or ``current_only``/``track_history``.
      - Strategy options via ``current_only_options``/``track_history_options`` or the generic
        ``change_tracking_options`` key.
    """

    change_tracking_mode, change_tracking_kwargs = _extract_tracking_kwargs_from_options(options)
    if change_tracking_mode == ChangeTrackingMode.CURRENT_ONLY:
        current_only_upsert(spark, source_df, target, **change_tracking_kwargs)
    else:
        track_history_upsert(spark, source_df, target, **change_tracking_kwargs)


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


def _write_append(df: DataFrame, target: str, *, merge_schema: bool = False) -> None:
    writer = df.write.format("delta").mode("append")
    if merge_schema:
        writer = writer.option("mergeSchema", "true")

    if _is_delta_path(target):
        writer.save(target)
    else:
        writer.saveAsTable(target)


@contextmanager
def _temporarily_enable_automerge(spark: SparkSession, enabled: bool):
    if not enabled:
        yield
        return

    key = "spark.databricks.delta.schema.autoMerge.enabled"
    had_prev = True
    try:
        prev_value = spark.conf.get(key)
    except Exception:
        had_prev = False
        prev_value = None

    spark.conf.set(key, "true")
    try:
        yield
    finally:
        if had_prev:
            spark.conf.set(key, prev_value)
        else:
            spark.conf.unset(key)


def _coalesce_cast_to_string(col: F.Column) -> F.Column:
    # Normalize None/Null -> empty string to ensure stable hashing; cast complex to string deterministically
    return F.coalesce(col.cast("string"), F.lit(""))


def _track_history_process_batch(
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
    allow_schema_evolution: bool,
) -> bool:
    """Apply track-history semantics for a batch that has at most one row per business key."""

    if not target_exists:
        if not create_if_not_exists:
            raise ValueError(f"Target '{target}' does not exist and create_if_not_exists=False")
        initial = (
            source_batch.withColumn(effective_col, ts_col)
            .withColumn(expiry_col, F.lit(None).cast("timestamp"))
            .withColumn(current_col, F.lit(True))
            .withColumn(version_col, F.lit(1).cast("bigint"))
        )
        _write_append(initial, target, merge_schema=allow_schema_evolution)
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

    _write_append(to_insert, target, merge_schema=allow_schema_evolution)
    return True


def current_only_upsert(
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
    allow_schema_evolution: bool = False,
) -> None:
    """Implement :class:`ChangeTrackingMode.CURRENT_ONLY`.

    Keeps exactly one active row per ``business_keys`` combination by:

    1. De-duplicating within the incoming batch using ``dedupe_keys``/``order_by``.
    2. Hashing the tracked columns to detect changes.
    3. Running a Delta ``MERGE`` that updates only when the row changed and inserts when missing.

    When ``allow_schema_evolution`` is ``True`` we temporarily enable Delta auto-merge so new source
    columns are added to the target on demand.
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
        _write_append(src_hashed, target, merge_schema=allow_schema_evolution)
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

    # Build column maps (exclude technical track-history columns if they exist in target)
    src_cols = src_hashed.columns
    # If target has history fields, do not attempt to write them during current-only merges
    history_fields = {"effective_start_ts", "effective_end_ts", "is_current", "version"}
    write_cols = [c for c in src_cols if c not in history_fields]

    set_map = {c: F.col(f"s.`{c}`") for c in write_cols}
    insert_map = {c: F.col(f"s.`{c}`") for c in write_cols}

    with _temporarily_enable_automerge(spark, allow_schema_evolution):
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


def track_history_upsert(
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
    allow_schema_evolution: bool = False,
) -> None:
    """Implement :class:`ChangeTrackingMode.TRACK_HISTORY`.

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
        allow_schema_evolution: When ``True``, append operations use Delta schema evolution so new
            columns added to the source DataFrame are automatically added to the target table. Only
            affects write paths (initial bootstrap + inserts).
    """

    if not business_keys:
        raise ValueError("business_keys must be a non-empty sequence")

    # Normalize/validate columns
    src_cols_set = set(source_df.columns)

    missing_keys = [k for k in business_keys if k not in src_cols_set]
    if missing_keys:
        raise ValueError(f"source_df missing business_keys: {missing_keys}")

    tracking_meta = {effective_col, expiry_col, current_col, version_col, hash_col}
    if tracked_columns is None:
        tracked_columns = [
            c for c in source_df.columns if c not in set(business_keys) | tracking_meta
        ]
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
        source_df = source_df.withColumn(CHANGE_TRACKING_SEQUENCE_COL, F.row_number().over(w))
    else:
        source_df = source_df.dropDuplicates(list(dedupe_keys)).withColumn(
            CHANGE_TRACKING_SEQUENCE_COL, F.lit(1)
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

    max_seq_val = source_hashed.agg(
        F.max(F.col(CHANGE_TRACKING_SEQUENCE_COL)).alias("__max_seq")
    ).collect()[0]["__max_seq"]

    if max_seq_val is None:
        if should_cache:
            source_hashed.unpersist()
        return

    create_flag = create_if_not_exists
    for seq in range(int(max_seq_val), 0, -1):
        batch = source_hashed.where(F.col(CHANGE_TRACKING_SEQUENCE_COL) == seq).drop(
            CHANGE_TRACKING_SEQUENCE_COL
        )
        target_exists = _track_history_process_batch(
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
            allow_schema_evolution=allow_schema_evolution,
        )
        create_flag = False

    if should_cache:
        source_hashed.unpersist()


def apply_change_tracking(
    spark: SparkSession,
    source_df: DataFrame,
    target: str,
    *,
    change_tracking_mode: Union[ChangeTrackingMode, str, int],
    **kwargs,
) -> None:
    """Unified entry point for change-tracking writes.

    ``change_tracking_mode`` accepts:
      - :class:`ChangeTrackingMode`
      - ``"current_only"`` / ``"track_history"``
      - ``1`` / ``2`` (handy when passing options via strings)
    """

    resolved = _resolve_mode(change_tracking_mode)
    if resolved == ChangeTrackingMode.CURRENT_ONLY:
        return current_only_upsert(spark, source_df, target, **kwargs)
    else:
        return track_history_upsert(spark, source_df, target, **kwargs)
