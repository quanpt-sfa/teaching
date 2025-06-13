"""
Module config chứa thông tin cấu hình chung cho package.
Các constants đã được di chuyển sang utils/constants.py và alias_maps.py để tránh circular import.
"""

from .utils.constants import (
    STAGE_RE, FUZZY_THRESHOLD,
    API_KEY, MODEL, EMBED_CACHE_FILE
)
from .utils.domain_dict import ACCOUNTING_TERMS, COMMON_SCHEMA_PATTERNS
from .utils.alias_maps import TABLE_ALIAS, SCHEMA_SYNONYMS

# Re-export ALIAS for backward compatibility
ALIAS = TABLE_ALIAS
