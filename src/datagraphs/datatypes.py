from enum import Enum

class DATATYPE(Enum):
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

    def __str__(self):
        return str(self.value)