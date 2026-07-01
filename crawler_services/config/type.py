from enum import Enum

class PageType(int, Enum):
    UNKNOWN = 0
    ANNOUNCEMENT = 1
    CURRICULUM = 2
    ENROLLMENT = 3
    FITINFO = 4


class URLType(int, Enum):
    UNKNOWN = 0
    HTML = 1
    PDF = 2
    DOCS = 3