import unicodedata
from .constants import RE_CAMEL, RE_NONAZ, RE_WS
from .alias_maps import TABLE_ALIAS, SCHEMA_SYNONYMS, build_bidirectional_synonyms

# Build synonym maps
_SYNONYMS = build_bidirectional_synonyms(SCHEMA_SYNONYMS)
_TABLE_ALIASES = build_bidirectional_synonyms(TABLE_ALIAS)

def normalize(txt: str) -> str:
    """Chuẩn hóa text: camelCase -> space, bỏ dấu, lowercase."""
    txt = RE_CAMEL.sub(r'\1 \2', txt).replace('_', ' ')
    txt = unicodedata.normalize('NFD', txt)
    txt = ''.join(c for c in txt if unicodedata.category(c) != 'Mn').lower()
    txt = RE_NONAZ.sub(' ', txt)
    return RE_WS.sub(' ', txt).strip()

def canonical(txt: str) -> str:
    """Chuẩn hóa và áp dụng alias nếu có."""
    norm = normalize(txt)
    # Thử table alias trước
    if norm in _TABLE_ALIASES:
        return _TABLE_ALIASES[norm]
    # Sau đó thử general synonym
    return _SYNONYMS.get(norm, norm)
