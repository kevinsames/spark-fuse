from spark_fuse.io.registry import list_connectors, connector_for_path


def test_connectors_registered():
    names = set(list_connectors())
    assert {"adls", "fabric", "databricks"}.issubset(names)


def test_connector_resolution_by_path():
    assert connector_for_path("abfss://c@acc.dfs.core.windows.net/p") is not None
    assert connector_for_path("onelake://ws/lh/Tables/tbl") is not None
    assert (
        connector_for_path("abfss://lh@onelake.dfs.fabric.microsoft.com/ws/lh/Tables/tbl")
        is not None
    )
    assert connector_for_path("dbfs:/mnt/delta") is not None
