import pyodbc

# Thông tin kết nối SQL Server (sửa lại cho phù hợp)
SERVER = 'QUANDESK\\MC22'
USER = 'sa'
PASSWORD = '123'
DATABASE = 'grading_schema_demo'

conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={SERVER};UID={USER};PWD={PASSWORD};TrustServerCertificate=yes;"

schema_sql = '''
-- Bảng lưu thông tin các bảng dữ liệu
IF OBJECT_ID(N'TableInfo', N'U') IS NULL
CREATE TABLE TableInfo (
    TableID INT IDENTITY(1,1),
    TableName NVARCHAR(128) NOT NULL,
    MSSV CHAR(8) NOT NULL, -- Mã sinh viên 8 ký số
    CONSTRAINT PK_TableInfo PRIMARY KEY (TableID, MSSV)
);

-- Bảng lưu thông tin các cột của bảng
IF OBJECT_ID(N'ColumnInfo', N'U') IS NULL
CREATE TABLE ColumnInfo (
    ColumnID INT IDENTITY(1,1),
    TableID INT NOT NULL,
    ColumnName NVARCHAR(128) NOT NULL,
    DataType NVARCHAR(64) NOT NULL,
    IsNullable NVARCHAR(8) NOT NULL,
    MSSV CHAR(8) NOT NULL, -- Mã sinh viên 8 ký số
    CONSTRAINT PK_ColumnInfo PRIMARY KEY (ColumnID, MSSV),
    FOREIGN KEY (TableID, MSSV) REFERENCES TableInfo(TableID, MSSV)
);

-- Bảng lưu thông tin quan hệ khóa ngoại
IF OBJECT_ID(N'ForeignKeyInfo', N'U') IS NULL
CREATE TABLE ForeignKeyInfo (
    FKID INT IDENTITY(1,1),
    FKName NVARCHAR(128) NOT NULL,
    ParentTable NVARCHAR(128) NOT NULL,
    FKColumn NVARCHAR(128) NOT NULL,
    RefTable NVARCHAR(128) NOT NULL,
    PKColumn NVARCHAR(128) NOT NULL,
    MSSV CHAR(8) NOT NULL, -- Mã sinh viên 8 ký số
    CONSTRAINT PK_ForeignKeyInfo PRIMARY KEY (FKID, MSSV)
);

-- Bảng lưu thông tin view và script
IF OBJECT_ID(N'ViewInfo', N'U') IS NULL
CREATE TABLE ViewInfo (
    ViewID INT IDENTITY(1,1),
    ViewName NVARCHAR(128) NOT NULL,
    ViewScript NVARCHAR(MAX) NOT NULL,
    MSSV CHAR(8) NOT NULL -- Mã sinh viên 8 ký số
    CONSTRAINT PK_ViewInfo PRIMARY KEY (ViewID, MSSV)
);

-- Bảng lưu thống kê số bản ghi trong các bảng dữ liệu
IF OBJECT_ID(N'TableRowCount', N'U') IS NULL
CREATE TABLE TableRowCount (
    TableID INT NOT NULL,
    RecordCount INT NOT NULL,
    MSSV CHAR(8) NOT NULL, -- Mã sinh viên 8 ký số
    CONSTRAINT PK_TableRowCount PRIMARY KEY (TableID, MSSV),
    FOREIGN KEY (TableID, MSSV) REFERENCES TableInfo(TableID, MSSV)
);

IF OBJECT_ID('Temp_Matching', 'U') IS NOT NULL DROP TABLE Temp_Matching;
CREATE TABLE Temp_Matching (
    MSSV CHAR(8),
    TableAnswer NVARCHAR(255),
    ColumnAnswer NVARCHAR(255),
    Result BIT,
    BestScore INT
);

IF OBJECT_ID('AbbreviationDict', 'U') IS NOT NULL DROP TABLE AbbreviationDict;
CREATE TABLE AbbreviationDict (
    Abbr NVARCHAR(128) NOT NULL PRIMARY KEY,
    FullWord NVARCHAR(255) NOT NULL
);

IF OBJECT_ID('SynonymDict', 'U') IS NOT NULL DROP TABLE SynonymDict;
CREATE TABLE SynonymDict (
    Word NVARCHAR(255) NOT NULL PRIMARY KEY,
    Synonym NVARCHAR(255) NOT NULL
);
'''

def create_schema():
    with pyodbc.connect(conn_str, autocommit=True) as conn:
        cursor = conn.cursor()
        # Đảm bảo tạo database nếu chưa có
        try:
            cursor.execute(f"IF DB_ID(N'{DATABASE}') IS NULL CREATE DATABASE [{DATABASE}];")
        except Exception as e:
            print(f"Lỗi khi tạo database: {e}")
        # Chuyển sang database grading_schema_demo
        try:
            cursor.execute(f"USE [{DATABASE}]")
        except Exception as e:
            print(f"Lỗi khi USE database: {e}")
        for stmt in schema_sql.split(';'):
            stmt = stmt.strip()
            if stmt:
                try:
                    cursor.execute(stmt)
                except Exception as e:
                    print(f"Lỗi khi thực thi: {stmt}\n{e}")
    print("Đã tạo xong schema lưu trữ thông tin grading.")

if __name__ == "__main__":
    create_schema()
