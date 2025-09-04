from spark_fuse.catalogs import hive, unity


def test_unity_sql_generation():
    assert (
        unity.create_catalog_sql("analytics")
        == "CREATE CATALOG IF NOT EXISTS `analytics`"
    )
    assert (
        unity.create_schema_sql("analytics", "core")
        == "CREATE SCHEMA IF NOT EXISTS `analytics`.`core`"
    )
    assert (
        unity.register_external_delta_table_sql("analytics", "core", "events", "abfss://x@y/p")
        == "CREATE TABLE IF NOT EXISTS `analytics`.`core`.`events` USING DELTA LOCATION 'abfss://x@y/p'"
    )


def test_hive_sql_generation():
    assert (
        hive.create_database_sql("analytics_core")
        == "CREATE DATABASE IF NOT EXISTS `analytics_core`"
    )
    assert (
        hive.register_external_delta_table_sql("analytics_core", "events", "abfss://x@y/p")
        == "CREATE TABLE IF NOT EXISTS `analytics_core`.`events` USING DELTA LOCATION 'abfss://x@y/p'"
    )

