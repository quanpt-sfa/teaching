"""Module xử lý tạo và lưu thông tin khóa ngoại vào bảng ForeignKeyInfo."""

def create_fk_info_table(conn):
    """Tạo bảng ForeignKeyInfo nếu chưa tồn tại."""
    try:
        sql = """
        IF OBJECT_ID('ForeignKeyInfo', 'U') IS NULL
        CREATE TABLE ForeignKeyInfo (
            ID INT IDENTITY(1,1) PRIMARY KEY,
            ParentTable NVARCHAR(128) NOT NULL,
            RefTable NVARCHAR(128) NOT NULL,
            FKColumns NVARCHAR(MAX) NOT NULL,
            PKColumns NVARCHAR(MAX) NOT NULL,
            FKName NVARCHAR(128) NOT NULL
        )
        """
        conn.execute(sql)
        conn.commit()
        return True
    except Exception as e:
        print(f"Error creating ForeignKeyInfo table: {e}")
        return False

def check_string_agg_support(conn):
    """Check if STRING_AGG function is supported"""
    try:
        conn.execute("SELECT STRING_AGG('test', ',')")
        return True
    except:
        return False

def save_foreign_keys(conn):
    """Lưu thông tin khóa ngoại từ sys.foreign_keys vào bảng ForeignKeyInfo."""
    if not create_fk_info_table(conn):
        return False
        
    try:
        # Xóa dữ liệu cũ
        conn.execute("DELETE FROM ForeignKeyInfo")
        
        # Use STRING_AGG if supported, otherwise use XML PATH as fallback
        if check_string_agg_support(conn):
            sql = """
            WITH FKInfo AS (
                SELECT 
                    fk.name AS FKName,
                    OBJECT_NAME(fkc.parent_object_id) AS ParentTable,
                    OBJECT_NAME(fkc.referenced_object_id) AS RefTable,
                    STRING_AGG(pc.name, ',') WITHIN GROUP (ORDER BY fkc.constraint_column_id) AS FKColumns,
                    STRING_AGG(rc.name, ',') WITHIN GROUP (ORDER BY fkc.constraint_column_id) AS PKColumns
                FROM sys.foreign_keys fk
                JOIN sys.foreign_key_columns fkc 
                    ON fk.object_id = fkc.constraint_object_id
                JOIN sys.columns pc 
                    ON fkc.parent_object_id = pc.object_id 
                    AND fkc.parent_column_id = pc.column_id
                JOIN sys.columns rc 
                    ON fkc.referenced_object_id = rc.object_id 
                    AND fkc.referenced_column_id = rc.column_id
                GROUP BY fk.name, fkc.parent_object_id, fkc.referenced_object_id
            )
            """
        else:
            sql = """
            WITH FKInfo AS (
                SELECT 
                    fk.name AS FKName,
                    OBJECT_NAME(fkc.parent_object_id) AS ParentTable,
                    OBJECT_NAME(fkc.referenced_object_id) AS RefTable,
                    STUFF((
                        SELECT ',' + pc2.name
                        FROM sys.foreign_key_columns fkc2
                        JOIN sys.columns pc2 
                            ON fkc2.parent_object_id = pc2.object_id 
                            AND fkc2.parent_column_id = pc2.column_id
                        WHERE fkc2.constraint_object_id = fk.object_id
                        ORDER BY fkc2.constraint_column_id
                        FOR XML PATH('')
                    ), 1, 1, '') AS FKColumns,
                    STUFF((
                        SELECT ',' + rc2.name
                        FROM sys.foreign_key_columns fkc2
                        JOIN sys.columns rc2 
                            ON fkc2.referenced_object_id = rc2.object_id 
                            AND fkc2.referenced_column_id = rc2.column_id
                        WHERE fkc2.constraint_object_id = fk.object_id
                        ORDER BY fkc2.constraint_column_id
                        FOR XML PATH('')
                    ), 1, 1, '') AS PKColumns
                FROM sys.foreign_keys fk
                JOIN sys.foreign_key_columns fkc 
                    ON fk.object_id = fkc.constraint_object_id
                GROUP BY fk.name, fkc.object_id, fkc.parent_object_id, fkc.referenced_object_id
            )
            """
            
        sql += """
        INSERT INTO ForeignKeyInfo (ParentTable, RefTable, FKColumns, PKColumns, FKName)
        SELECT ParentTable, RefTable, FKColumns, PKColumns, FKName
        FROM FKInfo
        """
        
        conn.execute(sql)
        conn.commit()
        return True
        
    except Exception as e:
        print(f"Error saving foreign keys: {e}")
        return False

def initialize_database(conn):
    """Khởi tạo cơ sở dữ liệu với các bảng cần thiết."""
    success = create_fk_info_table(conn)
    if success:
        success = save_foreign_keys(conn)
    return success
