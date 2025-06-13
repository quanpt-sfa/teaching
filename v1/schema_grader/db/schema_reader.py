import re
from .connection import open_conn
from ..config import STAGE_RE

def _clean_table_name(name: str) -> str:
    return re.sub(r'^\d+\.\s*', '', name)

def get_table_structures(conn):
    sql = """
        SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE LEFT(TABLE_NAME, 3) <> 'sys'
          AND TABLE_NAME IN (
            SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_TYPE='BASE TABLE' AND LEFT(TABLE_NAME,3) <> 'sys')
        ORDER BY TABLE_NAME, ORDINAL_POSITION"""
    rows = conn.cursor().execute(sql).fetchall()
    return [(_clean_table_name(t), c, d) for t, c, d in rows]

def get_primary_keys(conn):
    sql = """
        SELECT KU.TABLE_NAME, KU.COLUMN_NAME
        FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS TC
        JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS KU
            ON TC.CONSTRAINT_TYPE='PRIMARY KEY' AND TC.CONSTRAINT_NAME = KU.CONSTRAINT_NAME
        WHERE LEFT(KU.TABLE_NAME,3) <> 'sys'
        ORDER BY KU.TABLE_NAME, KU.ORDINAL_POSITION"""
    pk = {}
    for tbl, col in conn.cursor().execute(sql):
        pk.setdefault(tbl, []).append(col)
    return pk

def get_foreign_keys_full(conn):
    sql = """
        SELECT fk.name, tp.name, cp.name, ref.name, cr.name
        FROM sys.foreign_keys fk
        JOIN sys.tables tp    ON fk.parent_object_id     = tp.object_id
        JOIN sys.tables ref   ON fk.referenced_object_id = ref.object_id
        JOIN sys.foreign_key_columns fkc
             ON fk.object_id = fkc.constraint_object_id
        JOIN sys.columns cp   ON fkc.parent_object_id = cp.object_id
                             AND fkc.parent_column_id = cp.column_id
        JOIN sys.columns cr   ON fkc.referenced_object_id = cr.object_id
                             AND fkc.referenced_column_id = cr.column_id
        WHERE LEFT(tp.name,3)<>'sys' AND LEFT(ref.name,3)<>'sys'
        ORDER BY fk.name"""
    rows, out = conn.cursor().execute(sql).fetchall(), {}
    for fk, p_tbl, fk_col, r_tbl, pk_col in rows:
        key = (fk, p_tbl, r_tbl)
        out.setdefault(key, {
            'parent_tbl': p_tbl, 'parent_cols': [], 
            'ref_tbl': r_tbl,   'ref_cols': []})
        out[key]['parent_cols'].append(fk_col)
        out[key]['ref_cols'].append(pk_col)
    return list(out.values())
