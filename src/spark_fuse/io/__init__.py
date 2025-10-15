"""IO connectors and registry.

Import connector modules for side-effect registration so users/tests can rely on
`spark_fuse.io` importing all built-ins.
"""

# Ensure built-in connectors register with the plugin registry on import.
from . import azure_adls as _azure_adls  # noqa: F401
from . import fabric as _fabric  # noqa: F401
from . import databricks as _databricks  # noqa: F401
from . import qdrant as _qdrant  # noqa: F401
from . import rest_api as _rest_api  # noqa: F401
