"""DataGraphs Python client library."""

from importlib.metadata import version

from datagraphs.client import (
    AuthenticationError,
    Client,
    DatagraphsError,
    FailedWrite,
    HttpStatusError,
    PutResult,
)
from datagraphs.dataset import Dataset
from datagraphs.gateway import Gateway, LoadResult
from datagraphs.schema import Schema

__version__ = version("pydatagraphs")
"""The installed version of the pydatagraphs package, read from package metadata."""

__all__ = [
    "AuthenticationError",
    "Client",
    "DatagraphsError",
    "Dataset",
    "FailedWrite",
    "Gateway",
    "HttpStatusError",
    "LoadResult",
    "PutResult",
    "Schema",
    "__version__",
]
