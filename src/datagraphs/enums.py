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