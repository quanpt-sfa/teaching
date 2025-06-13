import os, re, pyodbc
from .connection import open_conn

def get_logical_file_names(bak_file, server, user, pw):
    with open_conn(server, user, pw, autocommit=True) as conn:
        rows = conn.cursor().execute(
            f"RESTORE FILELISTONLY FROM DISK = N'{bak_file}'"
        ).fetchall()
    if len(rows) < 2:
        raise ValueError("Backup thiếu logical file name")
    return rows[0][0], rows[1][0]

def normalize_path_for_sql(path):
    """Chuẩn hóa đường dẫn cho SQL Server: dùng backslash, escape đúng cách"""
    return os.path.abspath(path).replace('/', '\\')

def restore_database(bak_file, server, user, pw, data_folder) -> str:
    bak_name = os.path.basename(bak_file)
    db_name  = '00000001' if bak_name.lower() == 'dapan.bak' \
               else re.search(r'(\d{8})', bak_name).group(1) \
                    if re.search(r'(\d{8})', bak_name) else os.path.splitext(bak_name)[0]

    mdf_logical, ldf_logical = get_logical_file_names(bak_file, server, user, pw)
    
    # Đảm bảo đường dẫn đúng chuẩn Windows cho SQL Server
    bak_file = normalize_path_for_sql(bak_file)
    mdf_path = normalize_path_for_sql(os.path.join(data_folder, f"{db_name}.mdf"))
    ldf_path = normalize_path_for_sql(os.path.join(data_folder, f"{db_name}_log.ldf"))
    
    drop_sql = f"""
        IF DB_ID('{db_name}') IS NOT NULL BEGIN
            ALTER DATABASE [{db_name}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
            DROP DATABASE [{db_name}];
        END"""
    restore_sql = f"""
        RESTORE DATABASE [{db_name}] 
        FROM DISK = N'{bak_file}'
        WITH FILE = 1, 
             REPLACE,
             MOVE '{mdf_logical}' TO N'{mdf_path}',
             MOVE '{ldf_logical}' TO N'{ldf_path}',
             STATS = 10, 
             RECOVERY"""
    
    # Đảm bảo thư mục tồn tại
    os.makedirs(os.path.dirname(mdf_path), exist_ok=True)
    
    with open_conn(server, user, pw, autocommit=True) as conn:
        conn.execute(drop_sql)
        cur = conn.cursor()
        cur.execute(restore_sql)
        while cur.nextset(): pass
    return db_name
