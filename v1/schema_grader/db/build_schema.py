from collections import defaultdict

def build_schema_dict(rows, pk_dict, fk_list):
    """Xây dựng cấu trúc schema từ dữ liệu thô.
    
    Args:
        rows: List các tuple (Table, Column, Type)
        pk_dict: Dict {table: [primary_key_columns]}
        fk_list: List các foreign key info
    
    Returns:
        dict: Schema với cấu trúc {table: {'cols': [], 'pk': [], 'fks': []}}
    """
    schema = defaultdict(lambda: {'cols': [], 'pk': [], 'fks': []})
    for t, c, d in rows:
        schema[t]['cols'].append((c, d))
    for t, pkcols in pk_dict.items():
        schema[t]['pk'] = pkcols
    for fk in fk_list:
        schema[fk['parent_tbl']]['fks'].append(fk)
    return dict(schema)
