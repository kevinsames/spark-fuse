"""spark-fuse data source helpers."""

from .rest_api import (
    REST_API_CONFIG_OPTION,
    REST_API_FORMAT,
    REST_API_SCHEMA_OPTION,
    build_rest_api_config,
    register_rest_data_source,
)
from .sparql import (
    SPARQL_CONFIG_OPTION,
    SPARQL_DATA_SOURCE_NAME,
    SPARQL_SCHEMA_OPTION,
    build_sparql_config,
    register_sparql_data_source,
)

__all__ = [
    "REST_API_FORMAT",
    "REST_API_CONFIG_OPTION",
    "REST_API_SCHEMA_OPTION",
    "build_rest_api_config",
    "SPARQL_DATA_SOURCE_NAME",
    "SPARQL_CONFIG_OPTION",
    "SPARQL_SCHEMA_OPTION",
    "build_sparql_config",
    "register_rest_data_source",
    "register_sparql_data_source",
]
