from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Optional

import typer
from rich import box
from rich.panel import Panel
from rich.table import Table

from .. import __version__
from ..io import (
    REST_API_CONFIG_OPTION,
    REST_API_FORMAT,
    QDRANT_CONFIG_OPTION,
    QDRANT_FORMAT,
    SPARQL_CONFIG_OPTION,
    SPARQL_DATA_SOURCE_NAME,
    build_rest_api_config,
    build_qdrant_config,
    build_sparql_config,
    register_rest_data_source,
    register_qdrant_data_source,
    register_sparql_data_source,
)
from ..spark import create_session
from ..utils.logging import console


app = typer.Typer(name="spark-fuse", help="PySpark toolkit: connectors and CLI tools")

_DATA_SOURCES: Dict[str, str] = {
    "rest": "spark-fuse REST data source",
    "sparql": "spark-fuse SPARQL data source",
    "qdrant": "spark-fuse Qdrant data source",
}


@app.callback()
def _main(
    version: Optional[bool] = typer.Option(None, "--version", is_flag=True, help="Show version"),
):
    if version:
        console().print(f"spark-fuse {__version__}")
        raise typer.Exit(code=0)


@app.command("datasources")
def datasources_cmd():
    """List available spark-fuse data sources."""
    table = Table(title="Data Sources", box=box.SIMPLE_HEAVY)
    table.add_column("name")
    table.add_column("description")
    for name, desc in sorted(_DATA_SOURCES.items()):
        table.add_row(name, desc)
    console().print(table)


def _load_config_blob(value: Optional[str]) -> Dict[str, Any]:
    if not value:
        return {}
    candidate = Path(value)
    if candidate.exists():
        payload = candidate.read_text()
    else:
        payload = value
    try:
        return json.loads(payload)
    except json.JSONDecodeError as exc:
        raise typer.BadParameter(f"Invalid JSON payload: {exc}")


@app.command("read")
def read_cmd(
    path: str = typer.Option(..., help="Dataset path/URI"),
    data_source: str = typer.Option(..., "--format", "-f", help="Data source to use (rest|sparql)"),
    config: Optional[str] = typer.Option(
        None, help="Inline JSON or path to JSON config passed to the data source"
    ),
    show: int = typer.Option(5, help="Show N rows after load"),
):
    """Load and preview a dataset using one of spark-fuse's data sources."""

    fmt = data_source.lower()
    if fmt not in _DATA_SOURCES:
        console().print(Panel.fit(f"Unsupported data source: {data_source}", style="error"))
        raise typer.Exit(code=2)

    spark = create_session(app_name="spark-fuse-read")
    cfg = _load_config_blob(config)
    if fmt == "rest":
        register_rest_data_source(spark)
        payload = build_rest_api_config(spark, path, source_config=cfg)
        reader = spark.read.format(REST_API_FORMAT).option(
            REST_API_CONFIG_OPTION, json.dumps(payload)
        )
    elif fmt == "sparql":
        register_sparql_data_source(spark)
        payload = build_sparql_config(spark, path, source_config=cfg)
        reader = spark.read.format(SPARQL_DATA_SOURCE_NAME).option(
            SPARQL_CONFIG_OPTION, json.dumps(payload)
        )
    else:
        register_qdrant_data_source(spark)
        payload = build_qdrant_config(spark, path, source_config=cfg)
        reader = spark.read.format(QDRANT_FORMAT).option(QDRANT_CONFIG_OPTION, json.dumps(payload))

    df = reader.load()

    console().print(Panel.fit(f"Loaded with data source: {fmt}", title="Info", style="info"))
    df.show(show, truncate=False)
    console().print(f"Schema: {df.schema.simpleString()}")


if __name__ == "__main__":
    app()
