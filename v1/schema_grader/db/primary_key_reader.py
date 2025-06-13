"""Module xử lý việc đọc và lưu thông tin khóa chính từ database."""

from ..utils.constants import STAGE_RE
from .connection import open_conn

def get_primary_keys(conn, mssv: str = None):
    """Lấy thông tin khóa chính từ một database.
    
    Args:
        conn: Kết nối đến database cần đọc thông tin khóa chính
        mssv: Mã số sinh viên (nếu là database của sinh viên)
    
    Returns:
        list[tuple]: Danh sách (constraint_name, table_name, column_name, ordinal_position)
    """
    sql = """
        SELECT 
            TC.CONSTRAINT_NAME as pk_name,
            KU.TABLE_NAME as table_name,
            KU.COLUMN_NAME as column_name,
            KU.ORDINAL_POSITION as position
        FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS TC
        JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE KU 
            ON TC.CONSTRAINT_NAME = KU.CONSTRAINT_NAME
        WHERE TC.CONSTRAINT_TYPE = 'PRIMARY KEY'
            AND LEFT(KU.TABLE_NAME, 3) <> 'sys'
        ORDER BY KU.TABLE_NAME, KU.ORDINAL_POSITION"""
    
    cursor = conn.cursor()
    rows = cursor.execute(sql).fetchall()
    
    # Lọc bỏ các bảng stage nếu có
    return [
        (pk, tbl, col, pos) 
        for pk, tbl, col, pos in rows
        if not (STAGE_RE.match(tbl) or tbl.lower().startswith('stage'))
    ]

def save_primary_keys(grading_conn, source_conn, mssv: str):
    """Lưu thông tin khóa chính vào database grading.
    
    Args:
        grading_conn: Kết nối đến database grading để lưu thông tin
        source_conn: Kết nối đến database nguồn để đọc thông tin khóa chính
        mssv: Mã số sinh viên sở hữu schema
    """
    # Lấy thông tin khóa chính từ database nguồn
    pk_data = get_primary_keys(source_conn)
    
    # Lấy mapping TableID và ColumnID từ database grading
    cursor = grading_conn.cursor()
    
    # Lấy mapping table_name -> table_id
    cursor.execute("""
        SELECT TableName, TableID 
        FROM TableInfo 
        WHERE MSSV = ?
    """, mssv)
    table_ids = {name: id for name, id in cursor.fetchall()}
    
    # Lấy mapping (table_id, column_name) -> column_id
    cursor.execute("""
        SELECT c.TableID, c.ColumnName, c.ColumnID
        FROM ColumnInfo c
        JOIN TableInfo t ON c.TableID = t.TableID AND c.MSSV = t.MSSV
        WHERE c.MSSV = ?
    """, mssv)
    column_ids = {(tid, col): cid for tid, col, cid in cursor.fetchall()}
    
    # Chuẩn bị dữ liệu để insert
    values = []
    for pk_name, table_name, column_name, position in pk_data:
        # Lấy TableID và ColumnID
        table_id = table_ids.get(table_name)
        if table_id is None:
            print(f"Warning: Table {table_name} not found in grading database")
            continue
            
        column_id = column_ids.get((table_id, column_name))
        if column_id is None:
            print(f"Warning: Column {column_name} of table {table_name} not found in grading database")
            continue
            
        values.append((table_id, column_id, pk_name, table_name, column_name, position, mssv))
    
    if not values:
        return
        
    # Insert vào PrimaryKeyInfo
    cursor.executemany("""
        INSERT INTO PrimaryKeyInfo (
            TableID, ColumnID, PKName, TableName, ColumnName, 
            OrdinalPosition, MSSV
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
    """, values)
    
    grading_conn.commit()
