from __future__ import annotations

from typing import Iterable, Sequence, Tuple

import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .dataframe import ensure_columns

__all__ = [
    "to_pandas_sample",
    "plot_histogram",
    "plot_scatter",
    "plot_line",
    "plot_bar",
]

_DEFAULT_SAMPLE_LIMIT = 1000


def _lazy_import_matplotlib():
    try:
        import matplotlib.pyplot as plt  # type: ignore
    except ImportError as exc:  # pragma: no cover - exercised in user env
        raise ImportError(
            "matplotlib is required for visualization helpers. Install it via 'pip install matplotlib'."
        ) from exc
    return plt


def to_pandas_sample(
    df: DataFrame,
    *,
    columns: Sequence[str] | None = None,
    sample_fraction: float | None = None,
    sample_count: int | None = _DEFAULT_SAMPLE_LIMIT,
    seed: int | None = None,
) -> pd.DataFrame:
    """Collect a (potentially sampled) PySpark DataFrame as pandas data."""
    if sample_fraction is not None and not (0 < sample_fraction <= 1):
        raise ValueError("sample_fraction must be between 0 and 1.")
    if sample_count is not None and sample_count <= 0:
        raise ValueError("sample_count must be a positive integer.")

    working_df = df
    if columns:
        ensure_columns(df, columns)
        working_df = working_df.select(*columns)

    if sample_fraction is not None:
        working_df = working_df.sample(
            withReplacement=False,
            fraction=sample_fraction,
            seed=seed,
        )
    elif sample_count is not None:
        working_df = working_df.limit(sample_count)

    return working_df.toPandas()


def _prepare_axis(ax, figsize: Tuple[float, float] | None):
    if ax is not None:
        return ax
    plt = _lazy_import_matplotlib()
    _, axis = plt.subplots(figsize=figsize)
    return axis


def plot_histogram(
    df: DataFrame,
    column: str,
    *,
    bins: int = 20,
    sample_fraction: float | None = None,
    sample_count: int | None = _DEFAULT_SAMPLE_LIMIT,
    seed: int | None = None,
    ax=None,
    figsize: Tuple[float, float] = (8, 4),
    title: str | None = None,
    **hist_kwargs,
):
    """Plot a histogram for a numeric column from a PySpark DataFrame."""
    ensure_columns(df, [column])
    pdf = to_pandas_sample(
        df,
        columns=[column],
        sample_fraction=sample_fraction,
        sample_count=sample_count,
        seed=seed,
    )
    series = pdf[column].dropna()
    if series.empty:
        raise ValueError(f"No numeric data available in column {column!r} for plotting.")

    axis = _prepare_axis(ax, figsize)
    axis.hist(series, bins=bins, **hist_kwargs)
    axis.set_xlabel(column)
    axis.set_ylabel("count")
    axis.set_title(title or f"Distribution of {column}")
    return axis


def plot_scatter(
    df: DataFrame,
    x_col: str,
    y_col: str,
    *,
    color_col: str | None = None,
    legend: bool = True,
    sample_fraction: float | None = None,
    sample_count: int | None = _DEFAULT_SAMPLE_LIMIT,
    seed: int | None = None,
    ax=None,
    figsize: Tuple[float, float] = (6, 6),
    title: str | None = None,
    **scatter_kwargs,
):
    """Plot a scatter chart for two numeric columns."""
    cols: Iterable[str] = [x_col, y_col] + ([color_col] if color_col else [])
    ensure_columns(df, cols)

    pdf = to_pandas_sample(
        df,
        columns=list(cols),
        sample_fraction=sample_fraction,
        sample_count=sample_count,
        seed=seed,
    )

    if pdf.empty:
        raise ValueError("No data available for plotting.")

    axis = _prepare_axis(ax, figsize)
    if color_col:
        for value, group in pdf.groupby(color_col, dropna=False):
            label = "nan" if pd.isna(value) else str(value)
            axis.scatter(group[x_col], group[y_col], label=label, **scatter_kwargs)
        if legend:
            axis.legend(title=color_col)
    else:
        axis.scatter(pdf[x_col], pdf[y_col], **scatter_kwargs)

    axis.set_xlabel(x_col)
    axis.set_ylabel(y_col)
    axis.set_title(title or f"{y_col} vs {x_col}")
    return axis


def plot_line(
    df: DataFrame,
    x_col: str,
    y_col: str,
    *,
    order_by: str | None = None,
    sample_fraction: float | None = None,
    sample_count: int | None = _DEFAULT_SAMPLE_LIMIT,
    seed: int | None = None,
    ax=None,
    figsize: Tuple[float, float] = (8, 4),
    title: str | None = None,
    **line_kwargs,
):
    """Plot a line chart for numeric/temporal data."""
    cols = [x_col, y_col]
    if order_by and order_by not in cols:
        cols.append(order_by)
    ensure_columns(df, cols)

    pdf = to_pandas_sample(
        df,
        columns=cols,
        sample_fraction=sample_fraction,
        sample_count=sample_count,
        seed=seed,
    )
    if pdf.empty:
        raise ValueError("No data available for plotting.")

    order_column = order_by or x_col
    pdf = pdf.sort_values(by=order_column)

    axis = _prepare_axis(ax, figsize)
    axis.plot(pdf[x_col], pdf[y_col], **line_kwargs)
    axis.set_xlabel(x_col)
    axis.set_ylabel(y_col)
    axis.set_title(title or f"{y_col} over {x_col}")
    return axis


_AGG_FUNCS = {
    "sum": F.sum,
    "avg": F.avg,
    "mean": F.mean,
    "count": F.count,
    "max": F.max,
    "min": F.min,
}


def plot_bar(
    df: DataFrame,
    category_col: str,
    *,
    value_col: str | None = None,
    agg_func: str = "count",
    top_n: int | None = 10,
    descending: bool = True,
    ax=None,
    figsize: Tuple[float, float] = (8, 4),
    title: str | None = None,
    **bar_kwargs,
):
    """Plot a categorical aggregation as a bar chart."""
    ensure_columns(df, [category_col] + ([value_col] if value_col else []))

    if value_col is None:
        aggregated = df.groupBy(category_col).agg(F.count(F.lit(1)).alias("value"))
        value_label = "count"
    else:
        func = _AGG_FUNCS.get(agg_func.lower())
        if func is None:
            raise ValueError(f"Unknown aggregation function: {agg_func!r}")
        aggregated = df.groupBy(category_col).agg(func(F.col(value_col)).alias("value"))
        value_label = f"{agg_func.lower()}({value_col})"

    order_col = F.col("value").desc() if descending else F.col("value").asc()
    if top_n is not None:
        if top_n <= 0:
            raise ValueError("top_n must be a positive integer when provided.")
        aggregated = aggregated.orderBy(order_col).limit(top_n)
    else:
        aggregated = aggregated.orderBy(order_col)

    pdf = aggregated.toPandas()
    if pdf.empty:
        raise ValueError("No data available for plotting.")

    axis = _prepare_axis(ax, figsize)
    axis.bar(pdf[category_col].astype(str), pdf["value"], **bar_kwargs)
    axis.set_xlabel(category_col)
    axis.set_ylabel(value_label)
    axis.set_title(title or f"{value_label} by {category_col}")
    return axis
