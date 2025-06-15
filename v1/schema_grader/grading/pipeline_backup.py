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
from .row_count_checker import check_mapped_table_row_counts, format_row_count_results, analyze_row_counts
from .report_aggregator import aggregate_student_metrics, save_csv

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
                        )
                        
                        # Kiểm tra row count sau khi đã kiểm tra foreign key
                        row_count_results = None
                        if check_row_counts:
                            print(f"Checking row counts for {db_name}...")
                            row_count_results = check_mapped_table_row_counts(ans_conn, stu_conn, mapping, answer_schema)
                            
                            # Store results but don't save individual CSV files
                            print(f"Row count analysis completed for {db_name}")
                    else:
                        print(f"Warning: Foreign key info table initialization failed for {db_name}")
              
              # Kiểm tra row count ngay cả khi không có foreign key check
            row_count_results = None
            if check_row_counts and not fk_initialized:
                with connection.open_conn(server, user, pw, database='00000001') as ans_conn, \
                     connection.open_conn(server, user, pw, database=db_name) as stu_conn:
                    
                    print(f"Checking row counts for {db_name} (no FK check)...")
                    row_count_results = check_mapped_table_row_counts(ans_conn, stu_conn, mapping, answer_schema)
                    
                    # Store results but don't save individual CSV files
                    print(f"Row count analysis completed for {db_name}")
          # Tính điểm schema
        schema_score, table_results = calc_schema_score(answer_schema, student_schema)
        
        # Initialize row_count_results outside of conditional blocks
        final_row_count_results = None
        
        # Thêm thông tin row count vào kết quả
        result = {
            'db_name': db_name, 
            'schema_score': schema_score, 
            'table_results': table_results
        }
        
        # Check if row_count_results exists in any scope and add to result
        if 'row_count_results' in locals() and row_count_results:
            print(f"DEBUG: Found row_count_results for {db_name}")
            print(f"DEBUG: Row count summary keys: {list(row_count_results.get('summary', {}).keys())}")
            
            result['row_count_results'] = row_count_results
            result['business_logic_score'] = row_count_results.get('summary', {}).get('business_logic_score', 0)
            result['business_logic_max'] = row_count_results.get('summary', {}).get('business_logic_max', 0)
            result['business_logic_complete'] = row_count_results.get('summary', {}).get('business_logic_complete', False)
            final_row_count_results = row_count_results
        else:
            print(f"DEBUG: No row_count_results found for {db_name}")
        
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
    """Chấm hàng loạt file .bak trong thư mục"""    results = []
    
    # Collect raw data for aggregator
    column_match_data = {}  # MSSV -> list of column match results
    fk_match_data = {}      # MSSV -> list of FK match results  
    row_count_data = {}     # MSSV -> list of row count results
    
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
                
                # Collect data for summary report
                db_name = res.get('db_name')
                if db_name:
                    # Column matching rate - table_results is a list, not dict
                    table_results = res.get('table_results', [])
                    print(f"DEBUG: Column analysis for {db_name}")
                    print(f"DEBUG: table_results length: {len(table_results)}")
                    
                    if table_results:
                        print(f"DEBUG: Sample table_result: {table_results[0] if table_results else 'None'}")
                        
                        total_tables = len(table_results)
                        # Count tables that have enough matched columns
                        matched_tables = sum(1 for t in table_results if t.get('enough_cols', False))
                        
                        print(f"DEBUG: Total tables: {total_tables}, Matched: {matched_tables}")
                        print(f"DEBUG: enough_cols values: {[t.get('enough_cols', 'Missing') for t in table_results]}")
                        
                        column_match_rates[db_name] = matched_tables / total_tables if total_tables > 0 else 0.0
                    else:
                        print(f"DEBUG: No table_results for {db_name}")
                        column_match_rates[db_name] = 0.0
                    
                    # FK matching rate
                    fk_ratio = res.get('fk_ratio', 0.0)
                    fk_match_rates[db_name] = fk_ratio
                      # Row count results (using new format)
                    if 'row_count_results' in res and check_row_counts:
                        print(f"DEBUG: Processing row count for {db_name}")
                        
                        # Convert legacy format to new format for aggregator
                        legacy_rc = res['row_count_results']
                        summary = legacy_rc.get('summary', {})
                        
                        print(f"DEBUG: Legacy summary keys: {list(summary.keys())}")
                        print(f"DEBUG: Legacy summary values: {summary}")
                        
                        # Convert to new format expected by report_aggregator
                        total_mapped = summary.get('total_mapped_tables', 0)
                        exact_matches = summary.get('total_exact_matches', 0)
                        data_import_score = summary.get('data_import_score', 0)
                        
                        # Calculate partial success: tables that have data but not perfect
                        partial_success = max(0, data_import_score - exact_matches)
                        fail_count = max(0, total_mapped - data_import_score)
                        
                        new_format = {
                            "summary": {
                                "full_success": exact_matches,
                                "partial_success": partial_success, 
                                "fail": fail_count,
                                "tables_evaluated": total_mapped
                            }
                        }
                        
                        print(f"DEBUG: New format: {new_format}")
                        row_count_results[db_name] = new_format
                    else:
                        print(f"DEBUG: No row count results found for {db_name}, check_row_counts={check_row_counts}")
                        print(f"DEBUG: Result keys: {list(res.keys())}")
    
    # Generate summary report instead of individual files
    if results:
        # Generate aggregated summary
        summary_df = aggregate_student_metrics(column_match_rates, fk_match_rates, row_count_results)
        summary_path = os.path.join(out_dir, "student_summary.csv")
        save_csv(summary_df, summary_path)
        print(f"Summary report saved to {summary_path}")
        
        # Still save the detailed schema results for reference
        save_schema_results_csv(results, os.path.join(out_dir, "schema_grading_results.csv"))
    
    return results
