# LLM Mapping Demo

The `map_column_with_llm` helper (introduced in spark-fuse 0.2.0) normalizes free-form values by delegating disambiguation to an LLM with caching and batching.

```python
from spark_fuse.spark import create_session
from spark_fuse.utils.llm import map_column_with_llm

spark = create_session(app_name="spark-fuse-llm-demo")

df = spark.createDataFrame(
    [
        {"fruit": "apples"},
        {"fruit": "Banana"},
        {"fruit": "Cerry"},
    ]
)

standard = ["Apple", "Banana", "Cherry"]
mapped = map_column_with_llm(
    df,
    column="fruit",
    target_values=standard,
    model="o4-mini",
    temperature=None,
    dry_run=False,
)

mapped.select("fruit", "fruit_mapped").show()
```

Key features:

- Executor-side caching to avoid repeated API calls.
- Configurable batching and retry logic.
- Optional dry runs to gauge match rates before sending traffic.

## Notebook walkthrough

Explore the end-to-end workflow (including configuration tips) in the notebook:

- [LLM Mapping Demo](https://github.com/kevinsames/spark-fuse/blob/main/notebooks/demos/llm_mapping_demo.ipynb)
