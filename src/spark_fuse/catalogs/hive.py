from __future__ import annotations

from pyspark.sql import SparkSession


def _q(identifier: str) -> str:
    """Quote an identifier with backticks and escape internal backticks."""
    return f"`{identifier.replace('`', '``')}`"


def create_database_sql(database: str) -> str:
    """Return SQL to create a Hive database if it does not exist."""
    return f"CREATE DATABASE IF NOT EXISTS {_q(database)}"


def register_external_delta_table_sql(database: str, table: str, location: str) -> str:
    """Return SQL to register an external Delta table in the Hive metastore."""
    fq = f"{_q(database)}.{_q(table)}"
    return f"CREATE TABLE IF NOT EXISTS {fq} USING DELTA LOCATION '{location}'"


def create_database(spark: SparkSession, database: str) -> None:
    """Execute SQL to create the Hive database if missing."""
    spark.sql(create_database_sql(database))


def register_external_delta_table(
    spark: SparkSession, *, database: str, table: str, location: str
) -> None:
    """Create the database (if needed) and register an external Delta table."""
    create_database(spark, database)
    spark.sql(register_external_delta_table_sql(database, table, location))
