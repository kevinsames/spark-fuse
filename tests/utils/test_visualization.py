from __future__ import annotations

import pytest

from spark_fuse.utils.visualization import (
    plot_bar,
    plot_histogram,
    plot_line,
    plot_scatter,
    to_pandas_sample,
)


class DummyAxes:
    def __init__(self):
        self.hist_call = None
        self.scatter_calls = []
        self.line_call = None
        self.bar_call = None
        self.xlabel = None
        self.ylabel = None
        self.title = None
        self.legend_title = None

    def hist(self, data, bins=None, **kwargs):
        self.hist_call = {"data": list(data), "bins": bins, "kwargs": kwargs}

    def scatter(self, x, y, **kwargs):
        self.scatter_calls.append({"x": list(x), "y": list(y), "kwargs": kwargs})

    def plot(self, x, y, **kwargs):
        self.line_call = {"x": list(x), "y": list(y), "kwargs": kwargs}

    def bar(self, categories, values, **kwargs):
        self.bar_call = {"categories": list(categories), "values": list(values), "kwargs": kwargs}

    def set_xlabel(self, value):
        self.xlabel = value

    def set_ylabel(self, value):
        self.ylabel = value

    def set_title(self, value):
        self.title = value

    def legend(self, title=None):
        self.legend_title = title


@pytest.fixture
def sample_df(spark):
    return spark.createDataFrame(
        [
            (1, 10.0, "A"),
            (2, 20.0, "B"),
            (3, 30.0, "A"),
            (4, 40.0, "B"),
            (5, 50.0, "B"),
        ],
        ["x", "score", "group"],
    )


def test_to_pandas_sample_respects_limit(sample_df):
    pdf = to_pandas_sample(sample_df, columns=["x"], sample_count=2)
    assert len(pdf) == 2
    assert list(pdf.columns) == ["x"]


def test_to_pandas_sample_rejects_invalid_fraction(sample_df):
    with pytest.raises(ValueError, match="sample_fraction"):
        to_pandas_sample(sample_df, sample_fraction=1.5)


def test_plot_histogram_uses_provided_axis(sample_df):
    ax = DummyAxes()
    plot_histogram(sample_df, column="score", bins=5, ax=ax)

    assert ax.hist_call is not None
    assert ax.xlabel == "score"
    assert ax.ylabel == "count"
    assert ax.title == "Distribution of score"


def test_plot_scatter_groups_by_color(sample_df):
    ax = DummyAxes()
    plot_scatter(sample_df, "x", "score", color_col="group", ax=ax)

    assert len(ax.scatter_calls) == 2  # two groups
    labels = {call["kwargs"].get("label") for call in ax.scatter_calls}
    assert labels == {"A", "B"}
    assert ax.legend_title == "group"


def test_plot_line_orders_by_column(spark):
    df = spark.createDataFrame(
        [
            (2, 20),
            (1, 10),
            (3, 30),
        ],
        ["x", "y"],
    )
    ax = DummyAxes()
    plot_line(df, "x", "y", ax=ax)

    assert ax.line_call["x"] == [1, 2, 3]
    assert ax.line_call["y"] == [10, 20, 30]


def test_plot_bar_counts_categories(sample_df):
    ax = DummyAxes()
    plot_bar(sample_df, category_col="group", ax=ax)

    assert ax.bar_call["categories"] == ["B", "A"]
    assert ax.bar_call["values"] == [3, 2]
    assert ax.ylabel == "count"


def test_plot_bar_with_sum(sample_df):
    ax = DummyAxes()
    plot_bar(sample_df, category_col="group", value_col="score", agg_func="sum", ax=ax)

    assert ax.bar_call["categories"] == ["B", "A"]
    assert ax.bar_call["values"] == [110.0, 40.0]
    assert ax.ylabel == "sum(score)"


def test_plot_bar_invalid_agg(sample_df):
    with pytest.raises(ValueError, match="Unknown aggregation"):
        plot_bar(
            sample_df, category_col="group", value_col="score", agg_func="median", ax=DummyAxes()
        )
