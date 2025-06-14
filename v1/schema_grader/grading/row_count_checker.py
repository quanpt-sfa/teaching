"""
Row count analysis module for the grading system.

This module checks if students correctly implemented business logic
by comparing row counts between student and answer databases for ALL mapped tables.
"""

from typing import Dict, List, Tuple, Optional
from ..db import connection
from ..utils.log import get_logger

logger = get_logger(__name__)

# Expected business logic changes for the specific assignment
BUSINESS_LOGIC_CHANGES = {
    'NhaCungCap': 1,      # +1 Michael Đẹp trai
    'NhanVien': 1,        # +1 Mariya Sergienko
    'HangHoa': 1,         # +1 Crab Meat (12 - 4 oz tins)
    'MuaHang': 1,         # +1 Purchase order #71
    'ChiTietMuaHang': 1   # +1 Purchase detail for order #71
}

def get_table_row_count(conn, table_name: str) -> int:
    """Get row count for a specific table."""
    if not table_name or table_name == 'NOT_MAPPED' or table_name == 'ERROR_TABLE': # Added checks for invalid table names
        logger.warning(f"Invalid table name provided for row count: {table_name}")
        return -1
    try:
        cursor = conn.cursor()
        
        # Standard SQL Server quoting for table names that might contain spaces or special characters
        # The schema_reader and build_schema should provide the original, unquoted name.
        # Quoting here ensures it's handled correctly by the DB.
        # It's generally safer to always quote, as it doesn't harm simple names.
        quoted_table_name = f"[{table_name}]"

        try:
            # Attempt with just the quoted table name (most common if DB context is correct)
            cursor.execute(f"SELECT COUNT_BIG(*) FROM {quoted_table_name}") # Use COUNT_BIG for potentially large tables
            result = cursor.fetchone()
            count = result[0] if result else 0
            logger.debug(f"Table {quoted_table_name}: {count} rows")
            return count
        except Exception as e1:
            logger.debug(f"Failed querying {quoted_table_name}: {e1}. Trying with 'dbo' schema.")
            try:
                # Try with 'dbo' schema explicitly
                cursor.execute(f"SELECT COUNT_BIG(*) FROM dbo.{quoted_table_name}")
                result = cursor.fetchone()
                count = result[0] if result else 0
                logger.debug(f"Table dbo.{quoted_table_name}: {count} rows")
                return count
            except Exception as e2:
                logger.warning(f"All attempts failed for table {table_name} (tried {quoted_table_name} and dbo.{quoted_table_name}): {e1}, {e2}")
                return -1
                    
    except Exception as e:
        logger.error(f"Error counting rows in table '{table_name}': {e}")
        return -1

def check_mapped_table_row_counts(answer_conn, student_conn, table_mapping: Dict[str, Dict[str, Optional[str]]], answer_schema: Dict[str, Dict]) -> Dict:
    """
    Compare row counts for ALL mapped tables from stage 1 matching.
    The table_mapping is now {ans_cleaned_name: {'student_table': stu_cleaned_name, 'student_original_name': stu_original_name_from_db}}
    
    Logic:
    - For business logic tables: Check if difference matches expected changes
    - For regular data tables: Check if counts match exactly (data import correctness)
    - Overall assessment: Data import + Business logic implementation
    
    Args:
        answer_conn: Connection to answer database
        student_conn: Connection to student database  
        table_mapping: Dict of {answer_cleaned_table: {'student_table': student_cleaned_table, 'student_original_name': student_original_name_from_db}}
                       The answer_cleaned_table is also used to query the answer_db (assuming its original name is the same as cleaned or handled by get_table_row_count if it's simple).
                       For student tables, we MUST use 'student_original_name' for querying.
        answer_schema: Schema dict built by build_schema_dict, keys are cleaned table names, values include 'original_name'
                        The answer_cleaned_table is used only as a key; actual queries use answer_schema[...]['original_name'].

    Returns:
        Dict with comprehensive analysis results for ALL tables
    """
    
    logger.info(f"Analyzing row counts for {len(table_mapping)} mapped table pairs...")
    
    result = {
        'mapped_tables': {},
        'summary': {
            'total_mapped_tables': len(table_mapping),
            'total_exact_matches': 0,
            'total_business_tables': 0,
            'total_regular_tables': 0,
            'business_logic_score': 0,
            'business_logic_max': len(BUSINESS_LOGIC_CHANGES),
            'data_import_score': 0,
            'data_import_max': 0, # Will be set to total_regular_tables
            'all_tables_match': True, # Assume true until a mismatch
            'data_import_complete': False,
            'business_logic_complete': False,
            'unmapped_answer_tables': 0,
            'student_table_query_errors': 0,
        }
    }
    
    try:
        # The table_mapping keys are cleaned answer table names.
        # The values contain the cleaned student table name and the *original* student table name.
        # We assume answer table names for querying are their cleaned names (e.g., "NhaCungCap")
        # For student tables, we MUST use 'student_original_name' for querying.

        for ans_cleaned_table, stu_map_info in table_mapping.items():
            # Retrieve original answer table name for querying
            ans_original_table = answer_schema.get(ans_cleaned_table, {}).get('original_name', ans_cleaned_table)
            
            student_original_table = stu_map_info.get('student_original_name') if stu_map_info else None
            student_cleaned_table = stu_map_info.get('student_table') if stu_map_info else None

            table_info_template = {
                'answer_table_cleaned': ans_cleaned_table, # Cleaned name from answer schema
                'answer_table_original_for_query': ans_original_table,
                'student_table_cleaned': student_cleaned_table if student_cleaned_table else 'NOT_MAPPED',
                'student_table_original_for_query': student_original_table if student_original_table else 'NOT_MAPPED',
                'answer_count': 0,
                'student_count': 0,
                'difference': 0,
                'exact_match': False,
                'is_business_table': ans_cleaned_table in BUSINESS_LOGIC_CHANGES,
                'expected_increase': BUSINESS_LOGIC_CHANGES.get(ans_cleaned_table, 0),
                'error': None
            }

            if not student_original_table or student_original_table == 'NOT_MAPPED':
                logger.warning(f"No student table mapped for answer table: {ans_cleaned_table} - SKIPPING")
                table_info_template['error'] = 'No mapping found for student table'
                result['mapped_tables'][ans_cleaned_table] = table_info_template
                result['summary']['unmapped_answer_tables'] += 1
                result['summary']['all_tables_match'] = False # An unmapped table means not all tables match
                continue
            
            logger.info(f"Processing mapping: Answer original '{ans_original_table}' (cleaned '{ans_cleaned_table}') -> Student original '{student_original_table}'")
            
            # Get row counts
            # For answer_db, query using ans_cleaned_table.
            # For student_db, query using student_original_table.
            answer_count = get_table_row_count(answer_conn, ans_original_table)
            student_count = get_table_row_count(student_conn, student_original_table)
            
            logger.info(f"Row counts - Answer '{ans_original_table}': {answer_count}, Student '{student_original_table}': {student_count}")
            
            current_table_info = table_info_template.copy()
            current_table_info['answer_count'] = answer_count if answer_count != -1 else 0
            current_table_info['student_count'] = student_count if student_count != -1 else 0

            if answer_count == -1:
                logger.warning(f"Could not get row count for answer table: {ans_cleaned_table}")
                current_table_info['error'] = 'Could not get answer table row count'
                result['mapped_tables'][ans_cleaned_table] = current_table_info
                result['summary']['all_tables_match'] = False
                # Not incrementing student_table_query_errors as this is an answer table issue
                continue
                
            if student_count == -1:
                logger.warning(f"Could not get row count for student table: {student_original_table} (mapped from {ans_cleaned_table})")
                current_table_info['error'] = f'Could not get student table row count for {student_original_table}'
                result['mapped_tables'][ans_cleaned_table] = current_table_info
                result['summary']['student_table_query_errors'] += 1
                result['summary']['all_tables_match'] = False
                continue
                
            difference = student_count - answer_count
            exact_match = (difference == 0)
            
            current_table_info['difference'] = difference
            current_table_info['exact_match'] = exact_match
            
            result['mapped_tables'][ans_cleaned_table] = current_table_info # Store basic info
            
            if exact_match:
                result['summary']['total_exact_matches'] += 1
            else:
                result['summary']['all_tables_match'] = False # Any mismatch means not all tables match
            
            is_business_table = current_table_info['is_business_table']
            expected_increase = current_table_info['expected_increase']
            
            if is_business_table:
                result['summary']['total_business_tables'] += 1
                business_correct = False
                business_status = 'error_or_unmapped' # Default if error

                if current_table_info['error']: # If there was an error getting counts
                    business_status = 'error_counting_rows'
                elif difference == expected_increase: # Business logic applied, base data might be different
                    business_status = 'business_logic_correct'
                    business_correct = True
                    result['summary']['business_logic_score'] += 1
                elif exact_match and expected_increase == 0: # For business tables that shouldn't change, exact match is correct
                    business_status = 'correct_no_change_needed'
                    business_correct = True
                    result['summary']['business_logic_score'] += 1 # Or however you score these
                elif exact_match and expected_increase != 0: # Exact match but expected increase means BL not done
                     business_status = 'data_correct_business_logic_missing'
                     business_correct = False # BL not done
                else: # Incorrect
                    business_status = 'incorrect_business_logic'
                    business_correct = False
                
                current_table_info['business_status'] = business_status
                current_table_info['business_correct'] = business_correct
                logger.info(f"Business table {ans_cleaned_table}: Status={business_status}, Expected Inc={expected_increase}, Actual Diff={difference}")
                
            else: # Regular data table
                result['summary']['total_regular_tables'] += 1
                data_import_correct = False
                data_import_status = 'error_or_unmapped'

                if current_table_info['error']:
                     data_import_status = 'error_counting_rows'
                elif exact_match:
                    data_import_status = 'correct_data_import'
                    data_import_correct = True
                    result['summary']['data_import_score'] += 1
                else:
                    data_import_status = 'incorrect_data_import'
                    data_import_correct = False
                
                current_table_info['data_import_status'] = data_import_status
                current_table_info['data_import_correct'] = data_import_correct
                logger.info(f"Regular table {ans_cleaned_table}: Data import {data_import_status} (diff: {difference})")
        
        # Finalize summary scores
        result['summary']['data_import_max'] = result['summary']['total_regular_tables']
        if result['summary']['data_import_max'] > 0:
            result['summary']['data_import_complete'] = (
                result['summary']['data_import_score'] == result['summary']['data_import_max']
            )
        else: # No regular tables, so data import is vacuously complete
            result['summary']['data_import_complete'] = True 
        
        if result['summary']['business_logic_max'] > 0:
            result['summary']['business_logic_complete'] = (
                result['summary']['business_logic_score'] == result['summary']['business_logic_max']
            )
        else: # No business logic tables, so business logic is vacuously complete
             result['summary']['business_logic_complete'] = True

        # Overall status based on the new logic
        if result['summary']['unmapped_answer_tables'] > 0 or result['summary']['student_table_query_errors'] > 0:
            overall_status = 'critical_errors_mapping_or_querying'
        elif result['summary']['data_import_complete'] and result['summary']['business_logic_complete']:
            # Check if all tables truly matched if expected increases were involved
            all_perfect = True
            for table_data in result['mapped_tables'].values():
                if table_data.get('error'):
                    all_perfect = False; break
                if table_data['is_business_table']:
                    if table_data['difference'] != table_data['expected_increase']:
                        all_perfect = False; break
                elif not table_data['exact_match']: # Regular table
                    all_perfect = False; break
            overall_status = 'perfect_all_correct' if all_perfect else 'complete_with_business_logic'

        elif result['summary']['data_import_complete']:
            overall_status = 'data_complete_business_partial_or_incorrect'
        elif result['summary']['business_logic_complete']:
            overall_status = 'data_partial_or_incorrect_business_complete'
        else:
            overall_status = 'incomplete_data_and_business_logic'
            
        result['summary']['overall_status'] = overall_status
        
        logger.info(f"=== ROW COUNT ANALYSIS COMPLETE ===")
        logger.info(f"Total mapped tables: {result['summary']['total_mapped_tables']}")
        logger.info(f"  - Business logic tables: {result['summary']['total_business_tables']}")
        logger.info(f"  - Regular data tables: {result['summary']['total_regular_tables']}")
        logger.info(f"Exact matches: {result['summary']['total_exact_matches']}/{result['summary']['total_mapped_tables']}")
        logger.info(f"Data import score: {result['summary']['data_import_score']}/{result['summary']['data_import_max']}")
        logger.info(f"Business logic score: {result['summary']['business_logic_score']}/{result['summary']['business_logic_max']}")
        logger.info(f"Overall status: {result['summary']['overall_status']}")
        
    except Exception as e:
        logger.error(f"Error in row count analysis: {e}")
        result['error'] = str(e)
        result['summary']['all_tables_match'] = False
        result['summary']['business_logic_complete'] = False
        result['summary']['data_import_complete'] = False
    
    return result

def format_row_count_results(row_count_results: Dict, student_id: str) -> List[Dict]:
    """Format comprehensive row count results for CSV output."""
    
    csv_rows = []
    
    if 'error' in row_count_results:
        csv_rows.append({
            'MSSV': student_id,
            'Tên bảng đáp án': 'ERROR',
            'Tên bảng sinh viên': 'ERROR',
            'Số dòng đáp án': 0,
            'Số dòng sinh viên': 0,
            'Chênh lệch': 0,
            'Số dòng khớp': 0,
            'Đã nhập đúng nghiệp vụ': 0,
            'Là bảng nghiệp vụ': 0,
            'Điểm nghiệp vụ': '0/5',
            'Trạng thái': 'Lỗi',
            'Ghi chú': str(row_count_results['error'])
        })
        return csv_rows
      # Process all mapped tables
    mapped_tables = row_count_results.get('mapped_tables', {})
    summary = row_count_results.get('summary', {})
    
    for answer_table_cleaned, table_data in mapped_tables.items(): # Iterate using cleaned answer table name
        is_business = table_data.get('is_business_table', False)
        # exact_match = table_data.get('exact_match', False) # This alone is not enough for "Đã nhập đúng dữ liệu"
        answer_count = table_data.get('answer_count', 0)
        student_count = table_data.get('student_count', 0)
        difference = table_data.get('difference', 0)
        # Use the original student table name for display if available, else the cleaned one
        student_table_display = table_data.get('student_table_original_for_query', table_data.get('student_table_cleaned', 'N/A'))
        
        has_error = table_data.get('error') is not None
        
        status = 'N/A'
        note = ''
        data_imported_correctly_text = 'Không' # Default to No
        business_logic_done_text = 'Không'    # Default to No
        
        biz_score = summary.get('business_logic_score', 0)
        biz_max = summary.get('business_logic_max', len(BUSINESS_LOGIC_CHANGES)) # Use actual max
        business_score_text = f"'{biz_score}/{biz_max}" # Ensure string formatting for Excel

        if has_error:
            status = 'Lỗi'
            note = str(table_data['error'])
        else:
            if is_business:
                expected_increase = table_data.get('expected_increase', 0)
                # "Đã nhập đúng dữ liệu" for business table: student_count == answer_count (original data before BL)
                # This means difference == 0, but expected_increase might be > 0
                if student_count == answer_count : # Base data is correct
                    data_imported_correctly_text = 'Có'
                
                # "Đã nhập đúng nghiệp vụ": student_count == answer_count + expected_increase
                # This means difference == expected_increase
                if difference == expected_increase:
                    business_logic_done_text = 'Có'
                    status = 'Đúng nghiệp vụ'
                    if student_count == answer_count + expected_increase and expected_increase != 0 : # Both data and BL are correct
                         note = "Dữ liệu gốc và nghiệp vụ đều đúng."
                    elif student_count != answer_count + expected_increase and expected_increase !=0: # BL correct, but base data was wrong
                         note = "Nghiệp vụ đúng, nhưng dữ liệu gốc có thể sai."


                elif difference == 0 and expected_increase != 0: # Data correct, BL not done
                    status = 'Thiếu nghiệp vụ'
                    note = f"Đã nhập đủ dữ liệu gốc ({answer_count} dòng), nhưng thiếu {expected_increase} dòng nghiệp vụ."
                    # data_imported_correctly_text is already 'Có' if student_count == answer_count
                elif difference != expected_increase :
                    status = 'Sai nghiệp vụ'
                    note = f"Chênh lệch {difference}, kỳ vọng tăng {expected_increase}."
                    if student_count == answer_count: # Data correct, but BL is wrong (e.g. wrong number of rows added/removed)
                         note += " Dữ liệu gốc đúng, nhưng số dòng nghiệp vụ sai."


            else: # Regular data table
                if student_count == answer_count: # exact_match for non-business tables
                    data_imported_correctly_text = 'Có'
                    status = 'Khớp dữ liệu'
                    note = 'Số dòng khớp chính xác.'
                else:
                    status = 'Sai lệch dữ liệu'
                    note = f"Chênh lệch {difference} dòng so với đáp án."
            
            # Override note if student table was not found or error during count
            if student_table_display == 'NOT_MAPPED':
                status = 'Lỗi mapping'
                note = 'Không tìm thấy bảng tương ứng của sinh viên.'
            elif student_table_display == 'ERROR_TABLE' or "Could not get student table row count" in note: # Check specific error
                status = 'Lỗi truy vấn bảng SV'
                note = f"Không thể truy vấn số dòng bảng SV: {table_data.get('student_table_original_for_query', 'N/A')}. " + (table_data.get('error', ''))


        csv_rows.append({
            'MSSV': student_id,
            'Tên bảng đáp án': answer_table_cleaned, # Use cleaned name from answer schema
            'Tên bảng sinh viên': student_table_display, # Show original name used for query
            'Số dòng đáp án': answer_count if not has_error else 'Lỗi',
            'Số dòng sinh viên': student_count if not has_error else 'Lỗi',
            'Chênh lệch': difference if not has_error else 'Lỗi',
            'Đã nhập đúng dữ liệu': data_imported_correctly_text,
            'Đã nhập đúng nghiệp vụ': business_logic_done_text,
            'Là bảng nghiệp vụ': 'Có' if is_business else 'Không',
            'Điểm nghiệp vụ': business_score_text, # This is an overall score, repeated per row
            'Trạng thái': status,
            'Ghi chú': note
        })
        
    # Add a summary row if needed, or ensure the overall score is clear
    # For now, the 'Điểm nghiệp vụ' is repeated, which might be fine.
    
    return csv_rows

# ... (rest of the file)
