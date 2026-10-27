from enum import StrEnum


class HTTP(StrEnum):
    """Enumeration of HTTP methods available in the DataGraphs API."""
    GET = 'get'
    PUT = 'put'
    POST = 'post'
    DELETE = 'delete'

class DATATYPE(StrEnum):
    """Enumeration of property datatypes available in a DataGraphs schema."""
    TEXT = 'text'
    DATE = 'date'
    DATETIME = 'datetime'
    BOOLEAN = 'boolean'
    DECIMAL = 'decimal'
    INTEGER = 'integer'
    KEYWORD = 'keyword'
    URL = 'url'
    IMAGE_URL = 'imageUrl'
    ENUM = 'enum'

class VALIDATION_MODE(StrEnum):
    """Enumeration of validation modes for the DataGraphs project loading."""
    PROMPT = 'prompt'
    NO_PROMPT = 'no-prompt'
    BYPASS = 'bypass'

class REPORT_FORMAT(StrEnum):
    """Enumeration of output formats for a schema change report."""
    TEXT = 'text'
    RECORDS = 'records'

class SCHEMA_APPLY_MODE(StrEnum):
    VALIDATE_ONLY = 'validate'
    APPLY = 'apply'
    FORCE = 'force'

class ISOLATION_MODE(StrEnum):
    """How `Client.put` finds the entities the API rejected in a rejected batch.

    DataGraphs rejects a whole batch for any invalid entity, and does not
    reliably say which, so the rejected entities are found by resubmitting.
    """
    BISECT = 'bisect'
    """Resubmit the batch in halves, splitting each rejected half again until
    the rejected entities are isolated: about 2·log₂(batch size) requests per
    rejected entity. When most entities are rejected it switches to
    resubmitting them one at a time, so a batch never costs more than
    resubmitting it one entity at a time, plus 2·⌈log₂(batch size)⌉ requests."""
    INDIVIDUAL = 'individual'
    """Resubmit each entity of the batch on its own: one request per entity."""
    NONE = 'none'
    """Do not resubmit: report every entity of the batch as failed."""

