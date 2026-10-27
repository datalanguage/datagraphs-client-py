"""DataGraphs Python client library."""

from importlib.metadata import version

from datagraphs.client import (
    AuthenticationError,
    Client,
    DatagraphsError,
    FailedWrite,
    HttpStatusError,
    IncompleteReadError,
    PutResult,
    QueryPage,
    TimedOutWrite,
    TokenRequestError,
)
from datagraphs.dataset import Dataset
from datagraphs.gateway import DumpResult, Gateway, LoadResult, SkippedClass, SkipReason
from datagraphs.schema import Schema

__version__ = version("pydatagraphs")
"""The installed version of the pydatagraphs package, read from package metadata."""

__all__ = [
    "AuthenticationError",
    "Client",
    "DatagraphsError",
    "Dataset",
    "DumpResult",
    "FailedWrite",
    "Gateway",
    "HttpStatusError",
    "IncompleteReadError",
    "LoadResult",
    "PutResult",
    "QueryPage",
    "Schema",
    "SkipReason",
    "SkippedClass",
    "TimedOutWrite",
    "TokenRequestError",
    "__version__",
]
