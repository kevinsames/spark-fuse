from spark_fuse.io.fabric import FabricLakehouseConnector


def test_fabric_path_validation():
    c = FabricLakehouseConnector()
    assert c.validate_path("onelake://workspace/lakehouse/Tables/events")
    assert c.validate_path(
        "abfss://lakehouse@onelake.dfs.fabric.microsoft.com/workspace/lakehouse/Tables/events"
    )
    assert not c.validate_path("abfss://container@account.dfs.core.windows.net/path")

