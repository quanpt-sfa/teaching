import os
import pandas as pd
import traceback
from ..db import restore, connection, schema_reader
from ..db.clean_data import clean_rows
from ..db.build_schema import build_schema_dict
from ..db.drop_db import drop_database
from ..db.fk_info import initialize_database
from ..matching.table_matcher import phase1
from ..matching.column_matcher import phase2_one
from ..foreign_key.fk_matcher import compare_foreign_keys
from .schema_grader import calc_schema_score
from .reporter import save_schema_results_csv, save_row_count_summary
from .row_count_checker import check_mapped_table_row_counts, format_row_count_results

def run_for_one_bak(bak_path, server, user, pw, data_folder,
                    answer_schema, out_dir, check_row_counts=True) -> dict:
    """Chấm một file .bak"""
    db_name = None
    try:
        db_name = restore.restore_database(bak_path, server, user, pw, data_folder)
        if db_name == '00000001':
            return {}
            
        # Đọc schema sinh viên
        with connection.open_conn(server, user, pw, database=db_name) as conn:
            stu_struct = schema_reader.get_table_structures(conn)
            stu_struct = clean_rows(stu_struct)
            stu_pk = schema_reader.get_primary_keys(conn)
            stu_fk = schema_reader.get_foreign_keys_full(conn)
            
            # Khởi tạo bảng ForeignKeyInfo (bỏ qua nếu lỗi)
            fk_initialized = initialize_database(conn)
            
        student_schema = build_schema_dict(stu_struct, stu_pk, stu_fk)
        
        # Ghép bảng & cột
        mapping = phase1(answer_schema, student_schema)
        all_rows = []
        # mapping now maps cleaned answer table name to dict with student_table (cleaned name) and student_original_name
        for ans_tbl, map_info in mapping.items():
            stu_cleaned = map_info.get('student_table') if isinstance(map_info, dict) else None
            # Only proceed if a student table was matched
            if stu_cleaned:
                all_rows.extend(phase2_one(ans_tbl, stu_cleaned, answer_schema, student_schema))
            
        # Lưu kết quả chi tiết
        if all_rows:
            os.makedirs(out_dir, exist_ok=True)
            csv_path = os.path.join(out_dir, f"{db_name}_pairs.csv")
            pd.DataFrame(all_rows, columns=[
                'AnsTbl','AnsCol','AnsType','StuTbl','StuCol','StuType','Cos','Match'
            ]).to_csv(csv_path, index=False, encoding="utf-8-sig")
              # So khớp khóa ngoại nếu khởi tạo thành công
            fk_results = None
            fk_ratio = 0
            if fk_initialized:
                with connection.open_conn(server, user, pw, database='00000001') as ans_conn, \
                     connection.open_conn(server, user, pw, database=db_name) as stu_conn:
                        
                    # Khởi tạo bảng ForeignKeyInfo cho cả hai database
                    ans_ok = initialize_database(ans_conn)
                    stu_ok = initialize_database(stu_conn)
                    
                    if ans_ok and stu_ok:
                        fk_results, fk_ratio = compare_foreign_keys(
                            ans_conn, stu_conn, mapping,
                            os.path.join(out_dir, f"{db_name}_fk.csv")
                        )                        # Kiểm tra row count sau khi đã kiểm tra foreign key
                        row_count_results = None
                        if check_row_counts:
                            print(f"Checking row counts for {db_name}...")
                            row_count_results = check_mapped_table_row_counts(ans_conn, stu_conn, mapping, answer_schema)
                            
                            # Lưu kết quả row count
                            row_count_csv = format_row_count_results(row_count_results, db_name)
                            if row_count_csv:
                                row_count_df = pd.DataFrame(row_count_csv)
                                row_count_path = os.path.join(out_dir, f"{db_name}_rowcount.csv")
                                row_count_df.to_csv(row_count_path, index=False, encoding="utf-8-sig")
                                print(f"Row count results saved to {row_count_path}")
                    else:
                        print(f"Warning: Foreign key info table initialization failed for {db_name}")
              
              # Kiểm tra row count ngay cả khi không có foreign key check
            row_count_results = None
            if check_row_counts and not fk_initialized:
                with connection.open_conn(server, user, pw, database='00000001') as ans_conn, \
                     connection.open_conn(server, user, pw, database=db_name) as stu_conn:
                    
                    print(f"Checking row counts for {db_name} (no FK check)...")
                    row_count_results = check_mapped_table_row_counts(ans_conn, stu_conn, mapping, answer_schema)
                    
                    # Lưu kết quả row count
                    row_count_csv = format_row_count_results(row_count_results, db_name)
                    if row_count_csv:
                        row_count_df = pd.DataFrame(row_count_csv)
                        row_count_path = os.path.join(out_dir, f"{db_name}_rowcount.csv")
                        row_count_df.to_csv(row_count_path, index=False, encoding="utf-8-sig")
                        print(f"Row count results saved to {row_count_path}")
        
        # Tính điểm schema
        schema_score, table_results = calc_schema_score(answer_schema, student_schema)
        
        # Thêm thông tin row count vào kết quả
        result = {
            'db_name': db_name, 
            'schema_score': schema_score, 
            'table_results': table_results
        }
        
        # Thêm kết quả row count nếu có
        if 'row_count_results' in locals() and row_count_results:
            result['row_count_results'] = row_count_results
            result['business_logic_score'] = row_count_results.get('total_score', 0)
            result['business_logic_max'] = row_count_results.get('max_score', 0)
            result['business_logic_complete'] = row_count_results.get('business_logic_complete', False)
        
        # Thêm kết quả foreign key nếu có
        if 'fk_results' in locals() and fk_results:
            result['fk_results'] = fk_results
            result['fk_ratio'] = fk_ratio if 'fk_ratio' in locals() else 0
        
        return result
        
    except Exception as e:
        print(f"Lỗi với file {bak_path} (db_name={db_name}): {e}")
        traceback.print_exc()
        return {}
        
    finally:
        # Luôn xóa database khi xong việc
        if db_name:
            drop_database(server, user, pw, db_name)

def run_batch(bak_folder, answer_db_schema, server, user, pw, data_folder, out_dir, check_row_counts=True):
    """Chấm hàng loạt file .bak trong thư mục"""
    results = []
    for bak in os.listdir(bak_folder):
        ext = os.path.splitext(bak)[1].lower()
        if ext == '.bak' and bak.lower() != 'dapan.bak':
            bak_path = os.path.join(bak_folder, bak)
            if not os.path.isfile(bak_path):
                continue
            
            res = run_for_one_bak(bak_path, server, user, pw,
                                 data_folder, answer_db_schema, out_dir, check_row_counts)
            if res:
                results.append(res)
                
    save_schema_results_csv(results, os.path.join(out_dir, "schema_grading_results.csv"))
    
    # Save row count summary if any results have row count data
    if check_row_counts and any('row_count_results' in r for r in results):
        save_row_count_summary(results, out_dir)
    
    # Sau khi grading xong, sinh summary_table.csv từ các file csv đã sinh ra
    import subprocess
    summary_script = os.path.join(os.path.dirname(__file__), 'summary_table.py')
    summary_csv = os.path.join(out_dir, 'summary_table.csv')
    subprocess.run(['python', summary_script, out_dir, summary_csv], check=True)
    print(f"Summary table generated at {summary_csv}")
    
    return results
