"""DataGraphs Python client library."""

from importlib.metadata import version

from datagraphs.client import AuthenticationError, Client, DatagraphsError
from datagraphs.dataset import Dataset
from datagraphs.gateway import Gateway
from datagraphs.schema import Schema

__version__ = version("pydatagraphs")
"""The installed version of the pydatagraphs package, read from package metadata."""

__all__ = [
    "AuthenticationError",
    "Client",
    "DatagraphsError",
    "Dataset",
    "Gateway",
    "Schema",
    "__version__",
]
