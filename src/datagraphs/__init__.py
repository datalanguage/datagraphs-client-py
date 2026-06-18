"""DataGraphs Python client library."""

from importlib.metadata import version
from datagraphs.client import Client, AuthenticationError, DatagraphsError
from datagraphs.gateway import Gateway
from datagraphs.schema import Schema
from datagraphs.dataset import Dataset

__version__ = version("pydatagraphs")
"""The installed version of the pydatagraphs package, read from package metadata."""

__all__ = [
    "Client",
    "Gateway",
    "Schema",
    "Dataset",
    "AuthenticationError",
    "DatagraphsError",
    "__version__",
]
