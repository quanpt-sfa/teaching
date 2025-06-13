from ..db.connection import open_conn

def drop_database(server, user, pw, db_name):
    """Helper để xóa database sau khi dùng xong.
    
    Args:
        server: Tên server
        user: Username
        pw: Password
        db_name: Tên database cần xóa
    """
    if not db_name or db_name == '00000001':  # không xóa DB đáp án
        return
    try:
        drop_sql = f"""
            ALTER DATABASE [{db_name}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
            DROP DATABASE [{db_name}];
        """
        with open_conn(server, user, pw, autocommit=True) as conn:
            conn.execute(drop_sql)
    except Exception as e:
        print(f"Không thể xóa database {db_name}: {e}")
