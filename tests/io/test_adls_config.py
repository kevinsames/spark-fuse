from spark_fuse.io.azure_adls import ADLSGen2Connector


def test_adls_path_validation():
    c = ADLSGen2Connector()
    assert c.validate_path("abfss://container@account.dfs.core.windows.net/path/to/delta")
    assert not c.validate_path("wasbs://container@account.blob.core.windows.net/path")
    assert not c.validate_path("/local/path")
