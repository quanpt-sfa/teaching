import pyodbc, os, sys, re, unicodedata
import pandas as pd
from tkinter import Tk, filedialog, simpledialog
from rapidfuzz import fuzz
from functools import lru_cache
import numpy as np
import pickle
import hashlib
import requests
from scipy.optimize import linear_sum_assignment
import google.generativeai as genai

pyodbc.pooling = True

# --- Regex compile sẵn ---
RE_CAMEL  = re.compile(r'([a-z])([A-Z])')
RE_NONAZ  = re.compile(r'[^a-z0-9\s]')
RE_WS     = re.compile(r'\s+')
STAGE_RE = re.compile(r'^\d+\.')
ALIAS = {
    'chitien':'tratien',
    'ct_chitien':'ct_tratien',
    'chi tien':'tra tien',
    'pmh':'phieu muahang',
    'manv':'ma nhan vien',
    'sohd':'so hoa don',
    # … bổ sung tuỳ ý
}

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
    bak_name = os.path.basename(bak_file)
    bak_name_lower = bak_name.lower()
    if bak_name_lower == 'dapan.bak':
        db_name = '00000001'
    else:
        m = re.search(r'(\d{8})', bak_name)
        db_name = m.group(1) if m else os.path.splitext(bak_name)[0]
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

def get_primary_keys(conn):
    """Trả về dict: {TableName: [col1, col2, ...]}"""
    cursor = conn.cursor()
    cursor.execute('''
        SELECT KU.TABLE_NAME, KU.COLUMN_NAME
        FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS TC
        JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS KU
          ON TC.CONSTRAINT_TYPE = 'PRIMARY KEY' AND TC.CONSTRAINT_NAME = KU.CONSTRAINT_NAME
        WHERE LEFT(KU.TABLE_NAME, 3) <> 'sys'
        ORDER BY KU.TABLE_NAME, KU.ORDINAL_POSITION
    ''')
    pk_dict = {}
    for tbl, col in cursor.fetchall():
        pk_dict.setdefault(tbl, []).append(col)
    return pk_dict

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

def get_foreign_keys_full(conn):
    """Trả về list dict: mỗi dict gồm parent_tbl, parent_cols, ref_tbl, ref_cols"""
    cursor = conn.cursor()
    cursor.execute('''
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
    ''')
    rows = cursor.fetchall()
    # Gom các cột cùng 1 FK
    fk_map = {}
    for fk_name, parent_tbl, fk_col, ref_tbl, pk_col in rows:
        key = (fk_name, parent_tbl, ref_tbl)
        if key not in fk_map:
            fk_map[key] = {'parent_tbl': parent_tbl, 'parent_cols': [], 'ref_tbl': ref_tbl, 'ref_cols': []}
        fk_map[key]['parent_cols'].append(fk_col)
        fk_map[key]['ref_cols'].append(pk_col)
    return list(fk_map.values())

def restore_database(bak_file, server, user, password, data_folder):
    bak_name = os.path.basename(bak_file)
    bak_name_lower = bak_name.lower()
    if bak_name_lower == 'dapan.bak':
        db_name = '00000001'
    else:
        m = re.search(r'(\d{8})', bak_name)
        db_name = m.group(1) if m else os.path.splitext(bak_name)[0]
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

def get_primary_keys(conn):
    """Trả về dict: {TableName: [col1, col2, ...]}"""
    cursor = conn.cursor()
    cursor.execute('''
        SELECT KU.TABLE_NAME, KU.COLUMN_NAME
        FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS TC
        JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS KU
          ON TC.CONSTRAINT_TYPE = 'PRIMARY KEY' AND TC.CONSTRAINT_NAME = KU.CONSTRAINT_NAME
        WHERE LEFT(KU.TABLE_NAME, 3) <> 'sys'
        ORDER BY KU.TABLE_NAME, KU.ORDINAL_POSITION
    ''')
    pk_dict = {}
    for tbl, col in cursor.fetchall():
        pk_dict.setdefault(tbl, []).append(col)
    return pk_dict

def build_schema_dict(rows, pk_dict, fk_list):
    """rows: [(Table,Col,Type)], pk_dict: {tbl:[col]}, fk_list: list[dict]"""
    from collections import defaultdict
    schema = defaultdict(lambda: {'cols': [], 'pk': [], 'fks': []})
    for t, c, d in rows:
        schema[t]['cols'].append((c, d))
    for t, pkcols in pk_dict.items():
        schema[t]['pk'] = pkcols
    for fk in fk_list:
        schema[fk['parent_tbl']]['fks'].append(fk)
    return dict(schema)

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

def apply_alias(name: str) -> str:
    return ALIAS.get(name.lower(), name)

def clean_rows(raw_rows):
    out = []
    for raw_t, c, d in raw_rows:
        # 1. bỏ bảng stage **trên tên gốc**
        if STAGE_RE.match(raw_t) or raw_t.strip().lower().startswith('stage'):
            continue
        # 2. xoá tiền tố số sau khi đã lọc
        t = re.sub(r'^\d+\.\s*', '', raw_t)
        out.append((apply_alias(t), apply_alias(c), d))
    return out

# --- Embedding cache ---
EMBED_CACHE_FILE = 'embedding_cache.pkl'
try:
    with open(EMBED_CACHE_FILE, 'rb') as f:
        EMBED_CACHE = pickle.load(f)
except Exception:
    EMBED_CACHE = {}

API_KEY   = os.getenv("GEMINI_API_KEY", "AIzaSyBb-lZaEpKko9jLgKu7ZHUWssLMOmKyXK4")
genai.configure(api_key=API_KEY)
MODEL     = "models/text-embedding-004"

def get_vec(text):
    try:
        return _vec_cache(text)
    except Exception as e:
        print("Gemini ERROR:", e)
        return np.zeros(768, dtype=np.float32)

@lru_cache(maxsize=None)
def _vec_cache(text):
    key = hashlib.sha256(text.encode('utf-8')).hexdigest()
    if key in EMBED_CACHE:
        print(f"[gemini_embed] Cache hit for text: {text[:60]}... (key={key})")
        return EMBED_CACHE[key]
    print(f"[gemini_embed] Cache miss for text: {text[:60]}... (key={key})")
    try:
        vec = gemini_embed(text)
        print(f"[gemini_embed] API returned vector of length {len(vec)} for text: {text[:60]}...")
    except Exception as e:
        print(f"[gemini_embed] ERROR calling Gemini API for text: {text[:60]}...\n{e}")
        raise
    vec = np.array(vec, dtype=np.float32)
    norm = np.linalg.norm(vec) + 1e-8
    vec = vec / norm
    EMBED_CACHE[key] = vec
    try:
        with open(EMBED_CACHE_FILE, 'wb') as f:
            pickle.dump(EMBED_CACHE, f)
        print(f"[gemini_embed] Saved embedding to cache file for key={key}")
    except Exception as e:
        print(f"[gemini_embed] ERROR saving embedding cache: {e}")
    return vec

def gemini_embed(text: str) -> list[float]:
    print(f"[gemini_embed] Using Gemini SDK embed_content for: {text[:60]}...")
    try:
        result = genai.embed_content(
            model=MODEL,
            content=text,
            task_type="SEMANTIC_SIMILARITY"  # tốt hơn RETRIEVAL_DOCUMENT cho chuỗi ngắn
        )
        print(f"[gemini_embed] SDK result: {str(result)[:200]}...")
        return result['embedding']
    except Exception as e:
        print(f"[gemini_embed] ERROR in SDK call: {e}")
        raise

def calc_schema_score(answer_schema, student_schema, TBL_TH=0.80, COL_TH=0.82, SCORE_PER_TABLE=0.5):
    print("[calc_schema_score] Start schema grading with Gemini embeddings")
    ans_tbls = list(answer_schema.keys())
    stu_tbls = list(student_schema.keys())
    print(f"[calc_schema_score] Answer tables: {ans_tbls}")
    for t in ans_tbls:
        print(f"  [schema] {ser_table(t, answer_schema[t])}")
    print(f"[calc_schema_score] Student tables: {stu_tbls}")
    for t in stu_tbls:
        print(f"  [schema] {ser_table(t, student_schema[t])}")
    tbl_vec_ans = {t: get_vec(ser_table(t, m)) for t, m in answer_schema.items()}
    tbl_vec_stu = {t: get_vec(ser_table(t, m)) for t, m in student_schema.items()}
    A = np.stack([tbl_vec_ans[t] for t in ans_tbls])   # m × d
    B = np.stack([tbl_vec_stu[t] for t in stu_tbls])   # n × d

    sim_tbl = A @ B.T
    sim_tbl = sim_tbl / (np.linalg.norm(A, axis=1, keepdims=True) + 1e-8)
    sim_tbl = sim_tbl / (np.linalg.norm(B, axis=1, keepdims=True).T + 1e-8)

    # PATCH 1: chèn (m-n) cột đệm cosine=0
    m, n = sim_tbl.shape
    if n < m:
        sim_tbl = np.hstack([sim_tbl, np.zeros((m, m-n))])

    # PATCH 2: Lọc kết quả Hungarian & in đủ 8 bảng
    row, col = linear_sum_assignment(-sim_tbl)
    table_pairs = []          # cặp bảng thực (cos ≥ TBL_TH)
    report_rows = []          # để in đủ 8 bảng
    for r, c in zip(row, col):
        ans_t = ans_tbls[r]
        if c < n and sim_tbl[r, c] >= TBL_TH:         # bảng SV thật
            table_pairs.append((ans_t, stu_tbls[c], sim_tbl[r, c]))
            report_rows.append([ans_t, stu_tbls[c], sim_tbl[r, c]])
        else:                                        # rơi vào đệm
            report_rows.append([ans_t, "—", 0.0])

    hit_tables = 0
    table_results = []
    total_matched = 0
    for ans_t, stu_t, cos in table_pairs:
        ans_meta = answer_schema[ans_t]
        stu_meta = student_schema[stu_t]
        print(f"[calc_schema_score] Matching columns for: {ans_t} <-> {stu_t}")
        print(f"  [answer cols] {[ser_col(ans_t, c, d) for c, d in ans_meta['cols']]}")
        print(f"  [student cols] {[ser_col(stu_t, c, d) for c, d in stu_meta['cols']]}")
        ans_cols = ans_meta['cols']
        stu_cols = stu_meta['cols']
        if not ans_cols or not stu_cols:
            print("[WARN] Bỏ qua cặp bảng vì thiếu cột.")
            table_results.append({
                'ans_table': ans_t,
                'stu_table': stu_t,
                'cos': cos,
                'enough_cols': False,
                'pk_ok': False,
                'fk_ok': False,
                'matched_cols': 0
            })
            continue
        vA = safe_stack([get_vec(ser_col(ans_t, c, d)) for c, d in ans_cols])
        vB = safe_stack([get_vec(ser_col(stu_t, c, d)) for c, d in stu_cols])
        sim = vA @ vB.T
        sim = sim / (np.linalg.norm(vA, axis=1, keepdims=True) + 1e-8)
        sim = sim / (np.linalg.norm(vB, axis=1, keepdims=True).T + 1e-8)
        m, n = sim.shape
        size = max(m, n)
        cost = np.full((size, size), 1e3)
        cost[:m, :n] = -sim
        row, cidx = linear_sum_assignment(cost)
        matched = 0
        for i, j in zip(row, cidx):
            if i < m and j < n:
                # datatype phải TRÙNG (hoặc cùng họ)
                if sim[i, j] >= COL_TH and ans_cols[i][1].lower() == stu_cols[j][1].lower():
                    matched += 1
        ratio_cols = matched / len(ans_cols) if len(ans_cols) else 0
        enough_cols = ratio_cols >= 0.80
        pk_ok = set(ans_meta['pk']) == set(stu_meta['pk'])
        fk_ok = all(any(set(f['parent_cols']) == set(f2['parent_cols']) and set(f['ref_cols']) == set(f2['ref_cols']) and f['ref_tbl'] == f2['ref_tbl'] for f2 in stu_meta['fks']) for f in ans_meta['fks'])
        if enough_cols and pk_ok and fk_ok:
            hit_tables += 1
        table_results.append({
            'ans_table': ans_t,
            'stu_table': stu_t,
            'cos': cos,
            'enough_cols': enough_cols,
            'pk_ok': pk_ok,
            'fk_ok': fk_ok,
            'matched_cols': matched
        })
        total_matched += matched
    # Đảm bảo report đủ 8 bảng (kể cả bảng không ghép được)
    print("\n===== KẾT QUẢ BẢNG =====")
    for tA, tS, cs in report_rows:
        print(f"{tA:<18} ↔ {tS:<18} cos={cs:.2f}")
    # PATCH 4: Cộng điểm + thêm tỷ lệ cột đúng toàn DB
    total_ans_cols = sum(len(m['cols']) for m in answer_schema.values())
    ratio_all = total_matched / total_ans_cols if total_ans_cols else 0
    print(f"[TỶ LỆ CỘT ĐÚNG TOÀN DB] {ratio_all:.1%}")
    schema_score = hit_tables * SCORE_PER_TABLE
    print(f"[calc_schema_score] Final schema score: {schema_score}")
    return schema_score, table_results

def save_schema_results_csv(results, filename="schema_grading_results.csv"):
    """Lưu kết quả schema grading ra file CSV cho dễ kiểm tra."""
    import pandas as pd
    df = pd.DataFrame(results)
    df.to_csv(filename, index=False, encoding="utf-8-sig")
    print(f"[save_schema_results_csv] Đã lưu kết quả vào {filename}")

def ser_table(tbl, meta):
    txt = f"TABLE {tbl}: " + ", ".join(f"{c} ({t})" for c,t in meta['cols'])
    if meta['pk']: txt += " | PK:" + "_".join(meta['pk'])
    for fk in meta['fks'][:10]:
        txt += f" | FK {fk['parent_tbl']}({ '_'.join(fk['parent_cols']) })->{fk['ref_tbl']}({ '_'.join(fk['ref_cols']) })"
    return txt

def ser_col(tbl, col, typ):
    return f"{tbl}.{col} ({typ})"

def match_all_pairs(ans_schema, stu_schema, COL_TH=0.82):
    ans_tbls = list(ans_schema.keys())
    stu_tbls = list(stu_schema.keys())
    ans_items = [(t, c, d) for t, m in ans_schema.items() for c, d in m['cols']]
    stu_items = [(t, c, d) for t, m in stu_schema.items() for c, d in m['cols']]
    m, n = len(ans_items), len(stu_items)
    if m == 0 or n == 0:
        return [], 0.0, [], [], None
    vA = safe_stack([get_vec(ser_col(*it)) for it in ans_items])
    vB = safe_stack([get_vec(ser_col(*it)) for it in stu_items])
    sim = vA @ vB.T
    sim /= np.linalg.norm(vA, axis=1, keepdims=True)
    sim /= np.linalg.norm(vB, axis=1, keepdims=True).T
    size = max(m, n)
    cost = np.full((size, size), 1e3)
    cost[:m, :n] = -sim
    row, col = linear_sum_assignment(cost)
    pairs, correct = [], 0
    for i, j in zip(row, col):
        if i >= m or j >= n:
            continue
        tA, cA, dA = ans_items[i]
        tS, cS, dS = stu_items[j]
        score = sim[i, j]
        ok = score >= COL_TH and dA.lower() == dS.lower()
        if ok:
            correct += 1
        pairs.append({
            "AnsTbl": tA, "AnsCol": cA, "AnsType": dA,
            "StuTbl": tS, "StuCol": cS, "StuType": dS,
            "Cos": float(score), "Match": ok
        })
    ratio = correct / m
    # Bổ sung: tính ma trận tỉ lệ cột khớp giữa các bảng
    ans_tbls = list(ans_schema.keys())
    stu_tbls = list(stu_schema.keys())
    ratio_mat = np.zeros((len(ans_tbls), len(stu_tbls)))
    for row in pairs:
        if row['Match']:
            i = ans_tbls.index(row['AnsTbl'])
            j = stu_tbls.index(row['StuTbl'])
            ratio_mat[i, j] += 1
    for i, ans_t in enumerate(ans_tbls):
        ncol = len(ans_schema[ans_t]['cols'])
        if ncol > 0:
            ratio_mat[i, :] /= ncol
    return pairs, ratio, ans_tbls, stu_tbls, ratio_mat

def safe_stack(vecs):
    arr = np.vstack(vecs)
    if arr.ndim == 1:
        arr = arr.reshape(1, -1)
    return arr

from scipy.optimize import linear_sum_assignment

def refine_table_matching(answer_schema, student_schema, ans_tbls, stu_tbls, ratio_mat, db_name, COL_TH=0.82):
    m, n = ratio_mat.shape
    size = max(m, n)
    cost = np.full((size, size), 1.0)
    cost[:m, :n] = -ratio_mat
    row, col = linear_sum_assignment(cost)
    refined_pairs = []
    for i, j in zip(row, col):
        if i < m and j < n and ratio_mat[i, j] > 0:
            refined_pairs.append((ans_tbls[i], stu_tbls[j], ratio_mat[i, j]))
    # So khớp chi tiết lại từng cặp bảng
    final_pair_rows = []
    for ans_t, stu_t, ratio in refined_pairs:
        ans_cols = answer_schema[ans_t]['cols']
        stu_cols = student_schema[stu_t]['cols']
        if not ans_cols or not stu_cols:
            continue
        vA = safe_stack([get_vec(ser_col(ans_t, c, d)) for c, d in ans_cols])
        vB = safe_stack([get_vec(ser_col(stu_t, c, d)) for c, d in stu_cols])
        sim = vA @ vB.T
        sim = sim / (np.linalg.norm(vA, axis=1, keepdims=True) + 1e-8)
        sim = sim / (np.linalg.norm(vB, axis=1, keepdims=True).T + 1e-8)
        m1, n1 = sim.shape
        size1 = max(m1, n1)
        cost1 = np.full((size1, size1), 1e3)
        cost1[:m1, :n1] = -sim
        row1, col1 = linear_sum_assignment(cost1)
        for i1, j1 in zip(row1, col1):
            if i1 < m1 and j1 < n1:
                cA, dA = ans_cols[i1]
                cS, dS = stu_cols[j1]
                score = sim[i1, j1]
                ok = score >= COL_TH and dA.lower() == dS.lower()
                final_pair_rows.append({
                    "AnsTbl": ans_t, "AnsCol": cA, "AnsType": dA,
                    "StuTbl": stu_t, "StuCol": cS, "StuType": dS,
                    "Cos": float(score), "Match": ok
                })
    # Xuất csv
    import pandas as pd
    pd.DataFrame(final_pair_rows).to_csv(f"{db_name}_match_l2.csv", index=False, encoding="utf-8-sig")
    print(f"[refine_table_matching] Đã lưu kết quả refinement vào {db_name}_match_l2.csv")
    return final_pair_rows

def pick_best_student_table(ans_schema, stu_schema, COL_TH=0.7):
    ans_tbls = list(ans_schema.keys())
    stu_tbls = list(stu_schema.keys())
    ratio = np.zeros((len(ans_tbls), len(stu_tbls)))
    for i, a in enumerate(ans_tbls):
        a_cols = {c for c,_ in ans_schema[a]['cols']}
        for j, s in enumerate(stu_tbls):
            s_cols = {c for c,_ in stu_schema[s]['cols']}
            if len(a_cols) > 0:
                ratio[i, j] = len(a_cols & s_cols) / len(a_cols)
    best_map = {}
    for i, a in enumerate(ans_tbls):
        j = np.argmax(ratio[i])
        if ratio[i, j] > 0:
            best_map[a] = stu_tbls[j]
        else:
            best_map[a] = None
    return best_map

def grade_one_pair(ans_t, stu_t, ans_schema, stu_schema, COL_TH=0.82):
    ans_cols = ans_schema[ans_t]['cols']
    if stu_t is None:
        return [{"AnsTbl":ans_t, "AnsCol":c, "AnsType":d,
                 "StuTbl":"—",  "StuCol":"—", "StuType":"—",
                 "Cos":0.0, "Match":False} for c,d in ans_cols]
    stu_cols = stu_schema[stu_t]['cols']
    if not ans_cols or not stu_cols:
        return [{"AnsTbl":ans_t, "AnsCol":c, "AnsType":d,
                 "StuTbl":stu_t, "StuCol":"—", "StuType":"—",
                 "Cos":0.0, "Match":False} for c,d in ans_cols]
    vA = np.stack([get_vec(ser_col(ans_t,*c)) for c in ans_cols])
    vB = np.stack([get_vec(ser_col(stu_t,*c)) for c in stu_cols])
    if vA.ndim == 1:
        vA = vA.reshape(1, -1)
    if vB.ndim == 1:
        vB = vB.reshape(1, -1)
    sim = vA @ vB.T
    sim = sim / (np.linalg.norm(vA, axis=1, keepdims=True) + 1e-8)
    sim = sim / (np.linalg.norm(vB, axis=1, keepdims=True).T + 1e-8)
    size = max(len(ans_cols), len(stu_cols))
    cost = np.full((size,size), 1e3); cost[:len(ans_cols),:len(stu_cols)] = -sim
    row,col = linear_sum_assignment(cost)
    # Build assignment: i (ans_col idx) -> j (stu_col idx), only keep highest cos if duplicate j
    assign = {}
    for i, j in zip(row, col):
        if i < len(ans_cols) and j < len(stu_cols):
            # Only keep the highest cosine for each j
            if j not in assign.values() or sim[i, j] > sim[list(assign).index(j), j]:
                assign[i] = j
    rows = []
    used = set()
    for i, (a_c, a_ty) in enumerate(ans_cols):
        j = assign.get(i, None)
        if j is not None and j not in used:
            s_c, s_ty = stu_cols[j]
            cs = sim[i, j]
            ok = cs >= COL_TH and same_type(a_ty, s_ty, a_c, s_c)
            used.add(j)
        else:
            s_c = s_ty = "—"; cs = 0.0; ok = False
        rows.append({
            "AnsTbl": ans_t, "AnsCol": a_c, "AnsType": a_ty,
            "StuTbl": stu_t if j is not None and j < len(stu_cols) else "—",
            "StuCol": s_c,   "StuType": s_ty,
            "Cos": cs, "Match": ok
        })
    return rows

# ---------- Helpers --------------------------
def embed_vec(txt: str) -> np.ndarray:        # đã cache + L2-norm
    return get_vec(txt)                       # chính là hàm Gemini của bạn

def cosine_mat(A,B):
    # Ensure A and B are at least 2D
    A = np.atleast_2d(A)
    B = np.atleast_2d(B)
    # Normalize along axis=1 (row-wise)
    A_norm = np.linalg.norm(A, axis=1, keepdims=True) + 1e-8
    B_norm = np.linalg.norm(B, axis=1, keepdims=True) + 1e-8
    A = A / A_norm
    B = B / B_norm
    return A @ B.T
# ---------------------------------------------

# PHA 1  – ghép bảng
def phase1(ans_schema, stu_schema, TBL_TH=0.80):
    ans = list(ans_schema)
    stu = list(stu_schema)
    vecA = np.stack([
        embed_vec(f"TABLE {t}: "+", ".join(c for c,_ in ans_schema[t]['cols'][:30]))
        for t in ans
    ])
    vecB = np.stack([
        embed_vec(f"TABLE {t}: "+", ".join(c for c,_ in stu_schema[t]['cols'][:30]))
        for t in stu
    ])
    sim = cosine_mat(vecA, vecB)
    cost = np.full((max(len(ans),len(stu)),)*2, 1e3)
    cost[:len(ans),:len(stu)] = -sim
    r, c = linear_sum_assignment(cost)
    mapping = {}
    # Build a lookup for answer index to student index
    match_dict = {ri: ci for ri, ci in zip(r, c) if ri < len(ans) and ci < len(stu) and sim[ri, ci] >= TBL_TH}
    for i, ans_tbl in enumerate(ans):
        stu_idx = match_dict.get(i, None)
        if stu_idx is not None:
            mapping[ans_tbl] = stu[stu_idx]
        else:
            mapping[ans_tbl] = None  # không tìm được bảng SV
    return mapping
# PHA 2 – ghép cột
CODE_KEYWORDS = ("ma", "code", "id", "sohieu", "voucherid")
def is_code_column(col_name: str) -> bool:
    name = col_name.lower()
    return any(name.startswith(k) or name.endswith(k) for k in CODE_KEYWORDS)

TYPE_FAMILY = {
    'char':'str','varchar':'str','nvarchar':'str',
    'nchar':'str',
    'int':'int','bigint':'int','smallint':'int',
    'decimal':'num','numeric':'num','money':'num','real':'num','float':'num',
    'date':'dt','datetime':'dt','smalldatetime':'dt'
}

def same_type(atype: str, btype: str, col_a: str, col_b: str) -> bool:
    # Nếu đều là cột mã → cho int & str hoán đổi
    if is_code_column(col_a) and is_code_column(col_b):
        return atype.lower() in ('char','varchar','nvarchar','int','bigint','smallint') \
           and btype.lower() in ('char','varchar','nvarchar','int','bigint','smallint')
    # Bình thường: so “cùng họ”
    fam_a = TYPE_FAMILY.get(atype.lower(), atype.lower())
    fam_b = TYPE_FAMILY.get(btype.lower(), btype.lower())
    return fam_a == fam_b

def phase2_one(ans_tbl, stu_tbl, ans_schema, stu_schema,
               COL_TH=0.82):
    ans_cols = ans_schema[ans_tbl]['cols']
    if stu_tbl is None:
        return [(ans_tbl,c,d,'—','—','—',0.0,False) for c,d in ans_cols]
    stu_cols = stu_schema[stu_tbl]['cols']
    vA = np.stack([embed_vec(f"{ans_tbl}.{c} ({d})") for c,d in ans_cols])
    vB = np.stack([embed_vec(f"{stu_tbl}.{c} ({d})") for c,d in stu_cols])
    sim = cosine_mat(vA, vB)
    cost = np.full((max(len(ans_cols),len(stu_cols)),)*2, 1e3)
    cost[:len(ans_cols),:len(stu_cols)] = -sim
    r,c = linear_sum_assignment(cost)
    rows=[]
    for i,j in zip(r,c):
        if i>=len(ans_cols): break
        a_c,a_t = ans_cols[i]
        if j<len(stu_cols):
            s_c,s_t = stu_cols[j]; cs = sim[i,j]
            ok = cs>=COL_TH and same_type(a_t,s_t,a_c,s_c)
        else:
            s_c,s_t,cs,ok = '—','—',0.0,False
        rows.append([ans_tbl,a_c,a_t, stu_tbl if j<len(stu_cols) else '—',
                     s_c,s_t, cs, ok])
    return rows
# -------- Chạy cho mỗi sinh viên --------------
def main():
    import traceback
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
        root.destroy()
        # --- Tự động restore dapan.bak làm đáp án ---
        dapan_bak_path = None
        for f in os.listdir(bak_folder):
            if f.lower() == 'dapan.bak':
                dapan_bak_path = os.path.join(bak_folder, f)
                break
        if not dapan_bak_path or not os.path.isfile(dapan_bak_path):
            print("Không tìm thấy file dapan.bak trong thư mục đã chọn. Thoát chương trình.")
            sys.exit(1)
        try:
            answer_db = restore_database(dapan_bak_path, server, user, password, data_folder)
            print(f"[OK] Đã restore dapan.bak thành database đáp án: {answer_db}")
        except Exception as e:
            print(f"Lỗi khi restore dapan.bak: {e}")
            sys.exit(1)
        # --- Gemini schema grading ---
        try:
            master_conn = pyodbc.connect(get_connection_string(server, user, password))
            master_conn.autocommit = True
            master_conn.execute(f"USE [{answer_db}]")
            answer_struct = get_table_structures(master_conn)
            answer_struct = clean_rows(answer_struct)
            answer_pk = get_primary_keys(master_conn)
            answer_fk = get_foreign_keys_full(master_conn)
            answer_schema = build_schema_dict(answer_struct, answer_pk, answer_fk)
            master_conn.close()
        except Exception as e:
            print(f"Lỗi kết nối hoặc truy vấn đáp án mẫu: {e}")
            sys.exit(1)
        results = []
        grading_conn_str = get_connection_string(server, user, password, database="grading_schema_demo")
        all_stats = []
        out_dir = os.path.join(bak_folder, "pairs_out")
        os.makedirs(out_dir, exist_ok=True)
        for bak in os.listdir(bak_folder):
            # Sửa: nhận diện file .bak không phân biệt hoa thường
            ext = os.path.splitext(bak)[1].lower()
            if ext == '.bak':
                bak_path = os.path.join(bak_folder, bak)
                if not os.path.isfile(bak_path):
                    print(f"File không tồn tại: {bak_path}")
                    continue
                if bak.lower() == 'dapan.bak':
                    print(f"Bỏ qua file đáp án mẫu: {bak}")
                    continue
                db_name = None
                all_rows = []
                try:
                    db_name = restore_database(bak_path, server, user, password, data_folder)
                    print(f"[OK] {db_name} restored from {bak}")
                    if db_name == '00000001':
                        print(f"Bỏ qua database đáp án mẫu: {db_name}")
                        continue
                    master_conn = pyodbc.connect(get_connection_string(server, user, password))
                    master_conn.autocommit = True
                    master_conn.execute(f"USE [{db_name}]")
                    student_struct = get_table_structures(master_conn)
                    # 1 – Bỏ bảng clone rỗng ngay sau khi gom student_struct
                    cols_cnt = {}
                    for t,_,_ in student_struct:
                        cols_cnt[t] = cols_cnt.get(t,0)+1
                    student_struct = [ row for row in student_struct if cols_cnt[row[0]] > 0]
                    # 2 – clean_rows lọc stage trước khi bỏ tiền tố số
                    # ĐÃ CÓ clean_rows toàn cục, chỉ cần gọi:
                    student_struct = clean_rows(student_struct)
                    student_pk = get_primary_keys(master_conn)
                    student_fk = get_foreign_keys_full(master_conn)
                    student_schema = build_schema_dict(student_struct, student_pk, student_fk)
                    # 3 – Phase 1: match_tables for 1–1 mapping (Gemini only)
                    mapping = phase1(answer_schema, student_schema)
                    # Đảm bảo mỗi bảng đáp án đều xuất hiện, nếu không có cặp thì None (đã xử lý trong phase1)
                    # 4 – Phase 2: phase2_one, luôn xuất đủ bảng đáp án
                    all_rows = []
                    for ans_tbl, stu_tbl in mapping.items():          # đủ 8 bảng
                        all_rows.extend( phase2_one(ans_tbl, stu_tbl,
                                                    answer_schema, student_schema) )
                    if all_rows:
                        csv_path = os.path.join(out_dir, f"{db_name}_pairs.csv")
                        pd.DataFrame(all_rows, columns=[
                            'AnsTbl','AnsCol','AnsType','StuTbl','StuCol','StuType','Cos','Match'
                        ]).to_csv(csv_path, index=False, encoding="utf-8-sig")
                        print(f"[2-phase] Đã lưu kết quả 2-phase vào {csv_path}")
                    else:
                        print(f"[WARN] Không có dữ liệu để ghi cho {db_name} từ {bak}")
                    # ... giữ lại grading schema cũ nếu muốn ...
                    schema_score, table_results = calc_schema_score(answer_schema, student_schema)
                    print(f"[OK] {db_name} embedded, score={schema_score}")
                    print(f"== MSSV {db_name} - Schema {schema_score}/4 ==")
                    for r in table_results:
                        status = "ĐỦ" if r['enough_cols'] and r['pk_ok'] and r['fk_ok'] else "THIẾU"
                        print(f"{r['ans_table']:<18} ↔ {r['stu_table']:<18} cos={r['cos']:.2f}  {status}")
                        all_stats.append({
                            'MSSV': db_name,
                            'AnswerTable': r['ans_table'],
                            'StudentTable': r['stu_table'],
                            'Cosine': r['cos'],
                            'EnoughCols': r['enough_cols'],
                            'PK_OK': r['pk_ok'],
                            'FK_OK': r['fk_ok'],
                            'Status': status
                        })
                    results.append({'db_name': db_name, 'schema_score': schema_score})
                    master_conn.close()
                except Exception as e:
                    print(f"Lỗi với file {bak} (db_name={db_name}): {e}")
                    traceback.print_exc()
                finally:
                    if db_name and db_name != '00000001':
                        try:
                            drop_sql = f"ALTER DATABASE [{db_name}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE [{db_name}];"
                            with pyodbc.connect(get_connection_string(server, user, password), autocommit=True) as conn:
                                conn.execute(drop_sql)
                        except Exception as e:
                            print(f"Không thể xóa database {db_name}: {e}")
        # Sau vòng lặp, in log các file đã xuất
        print("\n[LOG] Các file pairs_out đã xuất:")
        for f in os.listdir(out_dir):
            print(f"  {f}")
        df = pd.DataFrame(results)
        print("\nKết quả chấm điểm schema:")
        print("==================")
        print(df)
        save_schema_results_csv(all_stats)
        total_bak = len([f for f in os.listdir(bak_folder) if f.lower().endswith('.bak')]) - 1
        print("Đã chấm", len(results), "sinh viên /", total_bak, "file .bak")
        try:
            drop_sql = f"ALTER DATABASE [00000001] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE [00000001];"
            with pyodbc.connect(get_connection_string(server, user, password), autocommit=True) as conn:
                conn.execute(drop_sql)
            print("[OK] Đã xóa database đáp án 00000001 sau khi grading.")
        except Exception as e:
            print(f"Không thể xóa database đáp án 00000001: {e}")
    except Exception as e:
        print(f"Lỗi: {e}")
    finally:
        debug_log.close()
        sys.stdout = sys.__stdout__

if __name__ == "__main__":
    main()