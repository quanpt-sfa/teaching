import pyodbc, os, sys, re, unicodedata
import pandas as pd
from tkinter import Tk, filedialog, simpledialog
from rapidfuzz import fuzz
from functools import lru_cache

pyodbc.pooling = True

# --- Regex compile sẵn ---
RE_CAMEL  = re.compile(r'([a-z])([A-Z])')
RE_NONAZ  = re.compile(r'[^a-z0-9\s]')
RE_WS     = re.compile(r'\s+')

@lru_cache(maxsize=None)
def normalize(txt: str) -> str:
    txt = RE_CAMEL.sub(r'\1 \2', txt).replace('_', ' ')
    txt = unicodedata.normalize('NFD', txt)
    txt = ''.join(c for c in txt if unicodedata.category(c) != 'Mn').lower()
    txt = RE_NONAZ.sub(' ', txt)
    return RE_WS.sub(' ', txt).strip()

# Global ABBR, SYN for canonical
ABBR = {}
SYN = {}

@lru_cache(maxsize=None)
def canonical(txt: str) -> str:
    base = normalize(txt)
    tokens = [ABBR.get(t, t) for t in base.split()]
    phrase = ' '.join(tokens)
    phrase = SYN.get(phrase, phrase)
    tokens2 = [SYN.get(t, t) for t in normalize(phrase).split()]
    return normalize(' '.join(tokens2))

def fuzzy_eq(a, b, th=80):
    ca, cb = canonical(a), canonical(b)
    score_token_set = fuzz.token_set_ratio(ca, cb)
    score_partial = fuzz.partial_ratio(ca, cb)
    score_ratio = fuzz.ratio(ca.replace(' ', ''), cb.replace(' ', ''))
    score = max(score_token_set, score_partial, score_ratio)
    print(f"[fuzzy_eq] Comparing: '{a}' <-> '{b}'")
    print(f"[fuzzy_eq] Canonical: '{ca}' <-> '{cb}'")
    print(f"[fuzzy_eq] Scores: token_set={score_token_set}, partial={score_partial}, ratio={score_ratio}, max={score}")
    print(f"[fuzzy_eq] Threshold: {th} => Result: {score >= th}")
    return score >= th, score

def get_connection_string(server, user, password, database="master"):
    return (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"UID={user};"
        f"PWD={password};"
        f"TrustServerCertificate=yes;"
    )

def get_logical_file_names(bak_file, server, user, password):
    conn_str = get_connection_string(server, user, password)
    with pyodbc.connect(conn_str, autocommit=True) as conn:
        cursor = conn.cursor()
        sql = f"RESTORE FILELISTONLY FROM DISK = N'{bak_file}'"
        cursor.execute(sql)
        rows = cursor.fetchall()
        if len(rows) < 2:
            raise Exception(f"File backup {bak_file} không hợp lệ hoặc thiếu logical file name.")
        mdf_logical = rows[0][0]
        ldf_logical = rows[1][0]
        return mdf_logical, ldf_logical

def restore_database(bak_file, server, user, password, data_folder):
    bak_name = os.path.basename(bak_file).lower()
    if bak_name == 'dapan.bak':
        db_name = '00000001'
    else:
        mssv = re.search(r'(\d{8})', os.path.basename(bak_file))
        if not mssv:
            raise Exception(f"Không tìm thấy MSSV 8 số trong tên file: {bak_file}")
        db_name = mssv.group(1)
    mdf_logical, ldf_logical = get_logical_file_names(bak_file, server, user, password)
    mdf_path = os.path.abspath(os.path.join(data_folder, f"{db_name}.mdf"))
    ldf_path = os.path.abspath(os.path.join(data_folder, f"{db_name}_log.ldf"))
    conn_str = get_connection_string(server, user, password)
    drop_sql = f"""
    IF DB_ID('{db_name}') IS NOT NULL
    BEGIN
        ALTER DATABASE [{db_name}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
        DROP DATABASE [{db_name}];
    END"""
    with pyodbc.connect(conn_str, autocommit=True) as conn:
        try:
            conn.execute(drop_sql)
        except pyodbc.Error:
            pass
    restore_sql = f"""
    RESTORE DATABASE [{db_name}]
    FROM DISK = N'{bak_file}'
    WITH FILE = 1,
         REPLACE,
         MOVE '{mdf_logical}' TO N'{mdf_path}',
         MOVE '{ldf_logical}' TO N'{ldf_path}',
         STATS = 10,
         RECOVERY
    """
    with pyodbc.connect(conn_str, autocommit=True) as restore_conn:
        cur = restore_conn.cursor()
        cur.execute(restore_sql)
        while cur.nextset():
            pass
    return db_name

def get_table_structures(conn):
    cursor = conn.cursor()
    cursor.execute("""
        SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE LEFT(TABLE_NAME, 3) <> 'sys'
          AND TABLE_NAME IN (
              SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND LEFT(TABLE_NAME, 3) <> 'sys'
          )
        ORDER BY TABLE_NAME, ORDINAL_POSITION
    """)
    rows = cursor.fetchall()
    def clean_table_name(name):
        return re.sub(r'^\d+\.\s*', '', name)
    return [(clean_table_name(t), c, d) for t, c, d in rows]

def get_foreign_keys(conn):
    cursor = conn.cursor()
    cursor.execute("""
        SELECT
            fk.name AS fk_name,
            tp.name AS parent_table,
            cp.name AS fk_column,
            ref.name AS ref_table,
            cr.name AS pk_column
        FROM sys.foreign_keys fk
        JOIN sys.tables tp ON fk.parent_object_id = tp.object_id
        JOIN sys.tables ref ON fk.referenced_object_id = ref.object_id
        JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
        JOIN sys.columns cp ON fkc.parent_object_id = cp.object_id AND fkc.parent_column_id = cp.column_id
        JOIN sys.columns cr ON fkc.referenced_object_id = cr.object_id AND fkc.referenced_column_id = cr.column_id
        WHERE LEFT(tp.name, 3) <> 'sys' AND LEFT(ref.name, 3) <> 'sys'
        ORDER BY fk.name, tp.name, cp.name
    """)
    return cursor.fetchall()

def get_views(conn):
    cursor = conn.cursor()
    cursor.execute("""
        SELECT TABLE_NAME, VIEW_DEFINITION
        FROM INFORMATION_SCHEMA.VIEWS
    """)
    return cursor.fetchall()

def get_row_counts(conn):
    cur = conn.cursor()
    cur.execute("""
        SELECT OBJECT_NAME(object_id), SUM(row_count)
        FROM sys.dm_db_partition_stats
        WHERE index_id IN (0,1)
        GROUP BY object_id
    """)
    return dict(cur.fetchall())

def load_abbr_syn_dict(grading_conn_str):
    global ABBR, SYN
    abbr = {}
    syn = {}
    with pyodbc.connect(grading_conn_str, autocommit=True) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT Abbr, FullWord FROM AbbreviationDict")
        for row in cursor.fetchall():
            abbr[row.Abbr.strip().lower()] = row.FullWord.strip().lower()
        cursor.execute("SELECT Word, Synonym FROM SynonymDict")
        for row in cursor.fetchall():
            raw_key = row.Word.strip()
            raw_value = row.Synonym.strip()
            key = normalize(raw_key)
            value = normalize(raw_value)
            syn[key] = value
    ABBR = abbr
    SYN = syn
    return abbr, syn

def insert_to_grading_db(mssv, table_struct, fks, views, row_counts, grading_conn_str):
    with pyodbc.connect(grading_conn_str, autocommit=True) as conn:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM TableRowCount WHERE MSSV=?", mssv)
        cursor.execute("DELETE FROM ColumnInfo WHERE MSSV=?", mssv)
        cursor.execute("DELETE FROM ForeignKeyInfo WHERE MSSV=?", mssv)
        cursor.execute("DELETE FROM ViewInfo WHERE MSSV=?", mssv)
        cursor.execute("DELETE FROM TableInfo WHERE MSSV=?", mssv)
        tableid_map = {}
        for tname, *_ in set([(t[0],) for t in table_struct]):
            cursor.execute("INSERT INTO TableInfo (TableName, MSSV) OUTPUT INSERTED.TableID VALUES (?, ?)", tname, mssv)
            tableid = cursor.fetchone()[0]
            tableid_map[tname] = tableid
        # Batch insert ColumnInfo
        cursor.fast_executemany = True
        cursor.executemany(
            "INSERT INTO ColumnInfo (TableID, ColumnName, DataType, IsNullable, MSSV) VALUES (?,?,?,?,?)",
            [(tableid_map[t], c, d, 'YES', mssv) for t,c,d in table_struct]
        )
        for fk_name, parent_table, fk_col, ref_table, pk_col in fks:
            def clean_fk_table_name(name):
                name = re.sub(r'^\d+\.\s*', '', name)
                name = re.sub(r'_(\d+)$', '', name)
                return name
            def clean_fk_name(name):
                name = re.sub(r'_(\d+)', '', name)
                name = name.replace('.', '_')
                return name
            def clean_col_name(name):
                return re.sub(r'_(\d+)$', '', name)
            clean_parent = clean_fk_table_name(parent_table)
            clean_ref = clean_fk_table_name(ref_table)
            clean_fk = clean_fk_name(fk_name)
            clean_fk_col = clean_col_name(fk_col)
            clean_pk_col = clean_col_name(pk_col)
            cursor.execute("INSERT INTO ForeignKeyInfo (FKName, ParentTable, FKColumn, RefTable, PKColumn, MSSV) VALUES (?, ?, ?, ?, ?, ?)",
                           clean_fk, clean_parent, clean_fk_col, clean_ref, clean_pk_col, mssv)
        for vname, vdef in views:
            cursor.execute("INSERT INTO ViewInfo (ViewName, ViewScript, MSSV) VALUES (?, ?, ?)", vname, vdef, mssv)
        for tname, count in row_counts.items():
            clean_tname = re.sub(r'^\d+\.\s*', '', tname)
            tableid = tableid_map.get(clean_tname)
            if tableid:
                cursor.execute("INSERT INTO TableRowCount (TableID, RecordCount, MSSV) VALUES (?, ?, ?)", tableid, count, mssv)

def compare_with_db(grading_conn_str, threshold=80):
    load_abbr_syn_dict(grading_conn_str)
    with pyodbc.connect(grading_conn_str, autocommit=True) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT MSSV FROM TableInfo WHERE MSSV <> '00000001'")
        mssv_list = [row[0] for row in cursor.fetchall()]
        cursor.execute("""
            SELECT t.TableName, c.ColumnName
            FROM TableInfo t
            JOIN ColumnInfo c ON t.TableID = c.TableID AND t.MSSV = c.MSSV
            WHERE t.MSSV = '00000001' AND LEFT(t.TableName, 5) <> 'stage'
        """)
        answer_columns = [(row.TableName, row.ColumnName) for row in cursor.fetchall()]
        answer_can = [(t, c, canonical(t), canonical(c)) for t, c in answer_columns]
        stats = []
        for mssv in mssv_list:
            from collections import defaultdict
            stu_index = defaultdict(list)
            cursor.execute("""
                SELECT t.TableName, c.ColumnName
                FROM TableInfo t
                JOIN ColumnInfo c ON t.TableID = c.TableID AND t.MSSV = c.MSSV
                WHERE t.MSSV = ? AND LEFT(t.TableName, 5) <> 'stage'
            """, mssv)
            for tname_stu, col_stu in cursor.fetchall():
                ctab = canonical(tname_stu)
                ccol = canonical(col_stu)
                stu_index[ctab].append((tname_stu, col_stu, ccol))
            for tname_ans, col_ans, ctab_ans, ccol_ans in answer_can:
                print(f"\n[compare_with_db] MSSV: {mssv} | Table: '{tname_ans}' | Column: '{col_ans}'")
                print(f"[compare_with_db] Canonical Table: '{ctab_ans}' | Canonical Column: '{ccol_ans}'")
                found = 0
                best_score = 0
                candidates = stu_index.get(ctab_ans, [])
                print(f"[compare_with_db] Candidates for table '{ctab_ans}': {[(t, c) for t, c, _ in candidates]}")
                if not candidates:
                    similar_tables = []
                    for key in stu_index:
                        if key.startswith('stage '):
                            continue
                        score = max(
                            fuzz.token_set_ratio(ctab_ans, key),
                            fuzz.ratio(ctab_ans, key),
                            fuzz.partial_ratio(ctab_ans, key)
                        )
                        if score >= 80:
                            similar_tables.append(key)
                    print(f"[compare_with_db] No direct candidates. Similar tables: {similar_tables}")
                    for key in similar_tables:
                        candidates.extend(stu_index[key])
                for t_stu, col_stu, ccol_stu in candidates:
                    print(f"[compare_with_db] Trying candidate: Table='{t_stu}', Column='{col_stu}', Canonical='{ccol_stu}'")
                    ok, score = fuzzy_eq(ccol_ans, ccol_stu, threshold)
                    if ok:
                        found = 1
                        best_score = score
                        print(f"[compare_with_db] Match found! Score={score}")
                        break
                    best_score = max(best_score, score)
                if not found:
                    print(f"[compare_with_db] No match found. Best score={best_score}")
                stats.append({'MSSV': mssv, 'TableAnswer': tname_ans, 'ColumnAnswer': col_ans, 'Result': found, 'BestScore': best_score})
        return stats

def insert_temp_matching(grading_conn_str, stats):
    with pyodbc.connect(grading_conn_str, autocommit=True) as conn:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM Temp_Matching")
        cursor.fast_executemany = True
        cursor.executemany(
            "INSERT INTO Temp_Matching (MSSV, TableAnswer, ColumnAnswer, Result, BestScore) VALUES (?, ?, ?, ?, ?)",
            [ (row['MSSV'], row['TableAnswer'], row['ColumnAnswer'], row['Result'], row['BestScore']) for row in stats ]
        )

def main():
    debug_log = open("debug_fuzzy_matching.txt", "w", encoding="utf-8")
    sys.stdout = debug_log
    try:
        root = Tk()
        root.withdraw()
        bak_folder = filedialog.askdirectory(title="Chọn thư mục chứa các file .bak")
        if not bak_folder:
            print("Bạn chưa chọn thư mục. Thoát chương trình.")
            sys.exit(1)
        data_folder = filedialog.askdirectory(title="Chọn thư mục lưu file .mdf/.ldf (DATA)")
        if not data_folder:
            print("Bạn chưa chọn thư mục DATA. Thoát chương trình.")
            sys.exit(1)
        server = simpledialog.askstring("Server", "Nhập tên server:", initialvalue="QUANDESK\\MC22")
        if server is None:
            print("Bạn đã hủy nhập server. Thoát chương trình.")
            sys.exit(1)
        user = simpledialog.askstring("User", "Nhập user:", initialvalue="sa")
        if user is None:
            print("Bạn đã hủy nhập user. Thoát chương trình.")
            sys.exit(1)
        password = simpledialog.askstring("Password", "Nhập password:", show='*',initialvalue="123")
        if password is None:
            print("Bạn đã hủy nhập password. Thoát chương trình.")
            sys.exit(1)
        answer_db = simpledialog.askstring("Answer DB", "Nhập tên database đáp án:", initialvalue="Mybot")
        if answer_db is None:
            print("Bạn đã hủy nhập tên database. Thoát chương trình.")
            sys.exit(1)
        root.destroy()
        try:
            master_conn = pyodbc.connect(get_connection_string(server, user, password))
            master_conn.autocommit = True
            master_conn.execute(f"USE [{answer_db}]")
            answer_struct = get_table_structures(master_conn)
            answer_fks = get_foreign_keys(master_conn)
            answer_views = get_views(master_conn)
            master_conn.close()
        except Exception as e:
            print(f"Lỗi kết nối hoặc truy vấn đáp án mẫu: {e}")
            sys.exit(1)
        results = []
        grading_conn_str = get_connection_string(server, user, password, database="grading_schema_demo")
        all_stats = []
        for bak in os.listdir(bak_folder):
            if bak.lower().endswith('.bak'):
                bak_path = os.path.join(bak_folder, bak)
                if not os.path.isfile(bak_path):
                    print(f"File không tồn tại: {bak_path}")
                    continue
                db_name = None
                try:
                    db_name = restore_database(bak_path, server, user, password, data_folder)
                    master_conn = pyodbc.connect(get_connection_string(server, user, password))
                    master_conn.autocommit = True
                    master_conn.execute(f"USE [{db_name}]")
                    student_struct = get_table_structures(master_conn)
                    student_fks = get_foreign_keys(master_conn)
                    student_views = get_views(master_conn)
                    row_counts = get_row_counts(master_conn)
                    insert_to_grading_db(db_name, student_struct, student_fks, student_views, row_counts, grading_conn_str)
                    stats = compare_with_db(grading_conn_str, threshold=80)
                    all_stats.extend(stats)
                    score = sum(s['Result'] for s in stats)
                    results.append({'db_name': db_name, 'score': score})
                    master_conn.close()
                except Exception as e:
                    print(f"Lỗi với file {bak}: {e}")
                finally:
                    if db_name:
                        try:
                            drop_sql = f"ALTER DATABASE [{db_name}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE [{db_name}];"
                            with pyodbc.connect(get_connection_string(server, user, password), autocommit=True) as conn:
                                conn.execute(drop_sql)
                        except Exception as e:
                            print(f"Không thể xóa database {db_name}: {e}")
        fuzzy_stats = compare_with_db(grading_conn_str, threshold=80)
        insert_temp_matching(grading_conn_str, fuzzy_stats)
        fuzzy_df = pd.DataFrame(fuzzy_stats)
        print("\nBẢNG TỔNG HỢP SO KHỚP FUZZY:")
        print(fuzzy_df)
        df = pd.DataFrame(results)
        print("\nKết quả chấm điểm:")
        print("==================")
        print(df)
        print("\nGhi chú điểm:")
        print("- 1 điểm cho cấu trúc bảng đúng")
        print("- 1 điểm cho khóa ngoại đúng")
        print("- 1 điểm cho view đúng")
    finally:
        sys.stdout = sys.__stdout__
        debug_log.close()
        print("Đã ghi log debug vào debug_fuzzy_matching.txt")

if __name__ == "__main__":
    main()