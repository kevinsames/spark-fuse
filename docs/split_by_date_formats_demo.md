# Split by Date Formats Demo

The `split_by_date_formats` transformation groups rows according to the date patterns present in a string column—helpful when cleaning mixed-format feeds.

```python
from spark_fuse.spark import create_session
from spark_fuse.utils.transformations import split_by_date_formats

spark = create_session(app_name="spark-fuse-date-split-demo")

df = spark.createDataFrame(
    [
        {"raw_date": "2024-01-01"},
        {"raw_date": "01/02/2024"},
        {"raw_date": "2024/03/05"},
    ]
)

splits = split_by_date_formats(
    df,
    column="raw_date",
    formats=["yyyy-MM-dd", "MM/dd/yyyy", "yyyy/MM/dd"],
)

for fmt, subset in splits.items():
    print(f"Format: {fmt}")
    subset.show()
```

Capabilities:

- Evaluates multiple formats in priority order.
- Returns a mapping of format → DataFrame for targeted downstream handling.
- Supports an optional `invalid_key` bucket for rows that do not match any pattern.

## Notebook walkthrough

Try the full example with additional parsing strategies in the notebook:

- [Split by Date Formats Demo](https://github.com/kevinsames/spark-fuse/blob/main/notebooks/demos/split_by_date_formats_demo.ipynb)
