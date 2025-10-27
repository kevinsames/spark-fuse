# Date & Time Dimensions

spark-fuse 0.3.0 introduces `create_date_dataframe` and `create_time_dataframe` helpers for quickly building reusable calendar and clock dimensions in PySpark.

## Date dimension

```python
from spark_fuse.utils.dataframe import create_date_dataframe

date_dim = create_date_dataframe(
    spark,
    start_date="2024-01-01",
    end_date="2024-01-07",
)

date_dim.select("date", "year", "week", "day_name").show()
```

Each row includes common calendar attributes:

- `date`
- `year`, `quarter`, `month`, `month_name`
- `week`, `day`, `day_of_week`, `day_name`

## Time dimension

```python
from spark_fuse.utils.dataframe import create_time_dataframe

time_dim = create_time_dataframe(
    spark,
    start_time="08:00:00",
    end_time="12:00:00",
    interval_seconds=30 * 60,
)

time_dim.select("time", "hour", "minute", "second").show()
```

The helper generates evenly spaced times between the provided bounds (inclusive) and adds:

- Formatted `time` string (`HH:MM:SS`)
- Integer `hour`, `minute`, and `second` columns

## Notebook walkthrough

Explore the full workflow in the interactive notebook:

- [Date & Time Dimensions Demo](https://github.com/kevinsames/spark-fuse/blob/main/notebooks/demos/date_time_dimensions_demo.ipynb)
