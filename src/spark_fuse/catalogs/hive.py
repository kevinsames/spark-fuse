from __future__ import annotations

from pyspark.sql import SparkSession


def _q(identifier: str) -> str:
    return f"`{identifier.replace('`', '``')}`"


def create_database_sql(database: str) -> str:
    return f"CREATE DATABASE IF NOT EXISTS {_q(database)}"


def register_external_delta_table_sql(database: str, table: str, location: str) -> str:
    fq = f"{_q(database)}.{_q(table)}"
    return f"CREATE TABLE IF NOT EXISTS {fq} USING DELTA LOCATION '{location}'"


def create_database(spark: SparkSession, database: str) -> None:
    spark.sql(create_database_sql(database))


def register_external_delta_table(
    spark: SparkSession, *, database: str, table: str, location: str
) -> None:
    create_database(spark, database)
    spark.sql(register_external_delta_table_sql(database, table, location))
