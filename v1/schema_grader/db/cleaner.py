import re
from ..config import STAGE_RE, ALIAS
from collections import defaultdict
from .apply_alias import apply_alias
from .build_schema import build_schema_dict
from .clean_data import clean_rows

__all__ = ['apply_alias', 'build_schema_dict', 'clean_rows']

def clean_rows(raw_rows):
    cleaned = []
    for raw_t, c, d in raw_rows:
        if STAGE_RE.match(raw_t) or raw_t.strip().lower().startswith('stage'):
            continue
        t = re.sub(r'^\d+\.\s*', '', raw_t)
        cleaned.append( (apply_alias(t), apply_alias(c), d) )
    return cleaned

def build_schema_dict(rows, pk_dict, fk_list):
    """rows: [(Table,Col,Type)], pk_dict: {tbl:[col]}, fk_list: list[dict]"""
    schema = defaultdict(lambda: {'cols': [], 'pk': [], 'fks': []})
    for t, c, d in rows:
        schema[t]['cols'].append((c, d))
    for t, pkcols in pk_dict.items():
        schema[t]['pk'] = pkcols
    for fk in fk_list:
        schema[fk['parent_tbl']]['fks'].append(fk)
    return dict(schema)
