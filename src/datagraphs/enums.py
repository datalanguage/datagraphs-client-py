from enum import StrEnum

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