from __future__ import annotations

from pyspark.sql import SparkSession


def _q(identifier: str) -> str:
    """Quote an identifier with backticks and escape internal backticks."""
    return f"`{identifier.replace('`', '``')}`"


def create_catalog_sql(catalog: str) -> str:
    """Return SQL to create a Unity Catalog catalog if it does not exist."""
    return f"CREATE CATALOG IF NOT EXISTS {_q(catalog)}"


def create_schema_sql(catalog: str, schema: str) -> str:
    """Return SQL to create a schema within a Unity Catalog catalog."""
    return f"CREATE SCHEMA IF NOT EXISTS {_q(catalog)}.{_q(schema)}"


def register_external_delta_table_sql(catalog: str, schema: str, table: str, location: str) -> str:
    """Return SQL to register an external Delta table in Unity Catalog."""
    fq = f"{_q(catalog)}.{_q(schema)}.{_q(table)}"
    return f"CREATE TABLE IF NOT EXISTS {fq} USING DELTA LOCATION '{location}'"


def create_catalog(spark: SparkSession, catalog: str) -> None:
    """Execute SQL to create the given Unity Catalog catalog if missing."""
    spark.sql(create_catalog_sql(catalog))


def create_schema(spark: SparkSession, catalog: str, schema: str) -> None:
    """Create the catalog (if needed) and schema in Unity Catalog."""
    create_catalog(spark, catalog)
    spark.sql(create_schema_sql(catalog, schema))


def register_external_delta_table(
    spark: SparkSession, *, catalog: str, schema: str, table: str, location: str
) -> None:
    """Register an external Delta table with Unity Catalog."""
    spark.sql(register_external_delta_table_sql(catalog, schema, table, location))
