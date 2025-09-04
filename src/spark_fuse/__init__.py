from importlib.metadata import PackageNotFoundError, version

__all__ = [
    "__version__",
]


def _pkg_version() -> str:
    try:
        return version("spark-fuse")
    except PackageNotFoundError:
        return "0.0.0"


__version__ = _pkg_version()
