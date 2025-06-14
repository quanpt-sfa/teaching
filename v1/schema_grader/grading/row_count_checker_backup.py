"""
Row count analysis module for the grading system.

This module checks if students correctly implemented business logic
by comparing row counts between student and answer databases.
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
    try:
        cursor = conn.cursor()
        
        # Try different approaches to handle table names
        try:
            # First try with brackets (SQL Server standard)
            cursor.execute(f"SELECT COUNT(*) FROM [{table_name}]")
            result = cursor.fetchone()
            count = result[0] if result else 0
            logger.debug(f"Table {table_name}: {count} rows (with brackets)")
            return count
        except Exception as e1:
            logger.debug(f"Failed with brackets for {table_name}: {e1}")
            
            try:
                # Try without brackets
                cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                result = cursor.fetchone()
                count = result[0] if result else 0
                logger.debug(f"Table {table_name}: {count} rows (without brackets)")
                return count
            except Exception as e2:
                logger.debug(f"Failed without brackets for {table_name}: {e2}")
                
                try:
                    # Try with schema prefix if needed
                    cursor.execute(f"SELECT COUNT(*) FROM dbo.[{table_name}]")
                    result = cursor.fetchone()
                    count = result[0] if result else 0
                    logger.debug(f"Table {table_name}: {count} rows (with dbo schema)")
                    return count
                except Exception as e3:
                    logger.warning(f"All attempts failed for table {table_name}: {e1}, {e2}, {e3}")
                    return -1
                    
    except Exception as e:
        logger.error(f"Error counting rows in {table_name}: {e}")
        return -1

def get_all_table_counts(conn) -> Dict[str, int]:
    """Get row counts for all tables in the database."""
    table_counts = {}
    
    try:
        cursor = conn.cursor()
        # Get all table names
        cursor.execute("""
            SELECT TABLE_NAME 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_TYPE = 'BASE TABLE'
            ORDER BY TABLE_NAME
        """)
        
        tables = [row[0] for row in cursor.fetchall()]
        logger.debug(f"Found {len(tables)} tables in database: {tables}")
        
        # Get count for each table
        for table in tables:
            count = get_table_row_count(conn, table)
            table_counts[table] = count
            logger.debug(f"Table '{table}': {count} rows")
            
    except Exception as e:
        logger.error(f"Error getting table counts: {e}")
        
    logger.info(f"Retrieved row counts for {len(table_counts)} tables")
    return table_counts

def get_real_table_names(conn) -> Dict[str, str]:
    """Get mapping from normalized names to real table names in database."""
    real_names = {}
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT TABLE_NAME 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_TYPE = 'BASE TABLE'
            ORDER BY TABLE_NAME
        """)
        
        tables = [row[0] for row in cursor.fetchall()]
        logger.debug(f"Real table names in database: {tables}")
        
        # Create mapping from normalized to real names
        for table in tables:
            # Try different normalizations that might be used in mapping
            clean_name = table
            # Remove numeric prefixes if any
            import re
            clean_name = re.sub(r'^\d+\.\s*', '', clean_name)
            real_names[clean_name.lower()] = table
            real_names[clean_name] = table
            real_names[table] = table  # Identity mapping
            
    except Exception as e:
        logger.error(f"Error getting real table names: {e}")
        
    return real_names

def check_mapped_table_row_counts(answer_conn, student_conn, table_mapping: Dict[str, str]) -> Dict:
    """
    Compare row counts for ALL mapped tables from stage 1 matching.
    
    Logic:
    - For business logic tables: Check if difference matches expected changes
    - For regular data tables: Check if counts match exactly (data import correctness)
    - Overall assessment: Data import + Business logic implementation
    
    Args:
        answer_conn: Connection to answer database
        student_conn: Connection to student database  
        table_mapping: Dict of {answer_table: student_table} from stage 1
    
    Returns:
        Dict with comprehensive analysis results for ALL tables
    """
    
    logger.info(f"Analyzing row counts for ALL {len(table_mapping)} mapped table pairs...")
    
    result = {
        'mapped_tables': {},  # All table analysis
        'business_logic_tables': {},  # Specific business logic analysis
        'regular_data_tables': {},  # Regular data tables analysis
        'summary': {
            'total_mapped_tables': len(table_mapping),
            'total_exact_matches': 0,
            'total_business_tables': 0,
            'total_regular_tables': 0,
            'business_logic_score': 0,
            'business_logic_max': len(BUSINESS_LOGIC_CHANGES),
            'data_import_score': 0,
            'data_import_max': 0,
            'all_tables_match': True,
            'data_import_complete': False,
            'business_logic_complete': False
        }
    }
    
    try:
        # Get real table names from both databases
        answer_real_names = get_real_table_names(answer_conn)
        student_real_names = get_real_table_names(student_conn)
        
        logger.info(f"Answer database real tables: {sorted(answer_real_names.values())}")
        logger.info(f"Student database real tables: {sorted(student_real_names.values())}")
          
        logger.info(f"Processing ALL {len(table_mapping)} mapped table pairs...")
        
        # Process each mapped table pair
        for answer_table, student_table in table_mapping.items():
            if student_table is None:
                logger.warning(f"No student table mapped for answer table: {answer_table}")
                continue
                
            logger.debug(f"Processing mapping: {answer_table} -> {student_table}")
            
            # Get real table names for querying
            real_answer_table = answer_real_names.get(answer_table, answer_table)
            real_student_table = student_real_names.get(student_table, student_table)
            
            logger.info(f"Table mapping: {answer_table} -> {student_table}")
            logger.info(f"Real names: {real_answer_table} -> {real_student_table}")
            
            # Get row counts using real table names
            answer_count = get_table_row_count(answer_conn, real_answer_table)
            student_count = get_table_row_count(student_conn, real_student_table)
            
            logger.info(f"Row counts - Answer '{real_answer_table}': {answer_count}, Student '{real_student_table}': {student_count}")
            
            if answer_count == -1:
                logger.warning(f"Could not get row count for answer table: {real_answer_table}")
                continue
            if student_count == -1:
                logger.warning(f"Could not get row count for student table: {real_student_table}")
                continue
                
            # Calculate difference and basic matching
            difference = student_count - answer_count
            exact_match = (difference == 0)
            
            # Determine if this is a business logic table
            is_business_table = answer_table in BUSINESS_LOGIC_CHANGES
            expected_increase = BUSINESS_LOGIC_CHANGES.get(answer_table, 0)
            
            # Create comprehensive table info
            table_info = {
                'answer_table': answer_table,
                'student_table': student_table,
                'real_answer_table': real_answer_table,
                'real_student_table': real_student_table,
                'answer_count': answer_count,
                'student_count': student_count,
                'difference': difference,
                'exact_match': exact_match,
                'is_business_table': is_business_table,
                'expected_increase': expected_increase if is_business_table else 0
            }
            
            # Store in main mapped tables collection
            result['mapped_tables'][answer_table] = table_info
            
            # Update summary counters
            if exact_match:
                result['summary']['total_exact_matches'] += 1
            else:
                result['summary']['all_tables_match'] = False
            
            # Process business logic tables
            if is_business_table:
                result['summary']['total_business_tables'] += 1
                
                if exact_match:
                    # Perfect match = both base data + business logic correct
                    business_status = 'complete_correct'
                    business_correct = True
                    result['summary']['business_logic_score'] += 1
                elif difference == expected_increase:
                    # Matches expected increase = business logic implemented correctly
                    business_status = 'business_logic_only'
                    business_correct = True
                    result['summary']['business_logic_score'] += 1
                else:
                    # Neither exact nor expected increase
                    business_status = 'incorrect'
                    business_correct = False
                
                table_info['business_status'] = business_status
                table_info['business_correct'] = business_correct
                
                # Store in business logic collection
                result['business_logic_tables'][answer_table] = {
                    'answer_count': answer_count,
                    'student_count': student_count,
                    'expected_increase': expected_increase,
                    'actual_difference': difference,
                    'exact_match': exact_match,
                    'business_correct': business_correct,
                    'status': business_status
                }
                
                logger.info(f"Business table {answer_table}: Status={business_status}, Expected={expected_increase}, Actual={difference}")
                
            else:
                # Regular data table - should match exactly for proper data import
                result['summary']['total_regular_tables'] += 1
                
                if exact_match:
                    result['summary']['data_import_score'] += 1
                    table_info['data_import_status'] = 'correct'
                else:
                    table_info['data_import_status'] = 'incorrect'
                
                # Store in regular data collection
                result['regular_data_tables'][answer_table] = {
                    'answer_count': answer_count,
                    'student_count': student_count,
                    'difference': difference,
                    'exact_match': exact_match,
                    'import_correct': exact_match
                }
                
                logger.info(f"Regular table {answer_table}: Data import {'correct' if exact_match else 'incorrect'} (diff: {difference})")
        
        # Calculate summary scores
        result['summary']['data_import_max'] = result['summary']['total_regular_tables']
        result['summary']['data_import_complete'] = (
            result['summary']['data_import_score'] == result['summary']['data_import_max']
        ) if result['summary']['data_import_max'] > 0 else True
        
        result['summary']['business_logic_complete'] = (
            result['summary']['business_logic_score'] == result['summary']['business_logic_max']
        )
        
        # Overall assessment
        total_correct = result['summary']['total_exact_matches']
        total_tables = result['summary']['total_mapped_tables']
        
        if result['summary']['all_tables_match']:
            overall_status = 'perfect_all_correct'
        elif result['summary']['data_import_complete'] and result['summary']['business_logic_complete']:
            overall_status = 'complete_with_business_logic'
        elif result['summary']['data_import_complete']:
            overall_status = 'data_complete_business_partial'
        elif result['summary']['business_logic_complete']:
            overall_status = 'data_partial_business_complete'
        else:
            overall_status = 'incomplete'
            
        result['summary']['overall_status'] = overall_status
        
        # Detailed logging
        logger.info(f"=== ROW COUNT ANALYSIS COMPLETE ===")
        logger.info(f"Total mapped tables: {result['summary']['total_mapped_tables']}")
        logger.info(f"  - Business logic tables: {result['summary']['total_business_tables']}")
        logger.info(f"  - Regular data tables: {result['summary']['total_regular_tables']}")
        logger.info(f"Exact matches: {result['summary']['total_exact_matches']}/{total_tables}")
        logger.info(f"Data import score: {result['summary']['data_import_score']}/{result['summary']['data_import_max']}")
        logger.info(f"Business logic score: {result['summary']['business_logic_score']}/{result['summary']['business_logic_max']}")
        logger.info(f"Overall status: {overall_status}")        
    except Exception as e:
        logger.error(f"Error in row count analysis: {e}")
        result['error'] = str(e)
        result['summary']['all_tables_match'] = False
        result['summary']['business_logic_complete'] = False
        result['summary']['data_import_complete'] = False
    
    return result

def check_all_table_row_counts(answer_conn, student_conn) -> Dict:
        Dict with comprehensive analysis results
    """
    
    logger.info("Checking row counts for all tables...")
    
    result = {
        'all_tables': {},
        'business_logic_tables': {},
        'business_logic_score': 0,
        'business_logic_max': len(BUSINESS_LOGIC_CHANGES),
        'business_logic_complete': False,
        'total_tables_compared': 0,
        'total_tables_matched': 0,
        'all_tables_match': True
    }
    
    try:
        # Get row counts from both databases
        answer_counts = get_all_table_counts(answer_conn)
        student_counts = get_all_table_counts(student_conn)
        
        logger.info(f"Found {len(answer_counts)} tables in answer database")
        logger.info(f"Found {len(student_counts)} tables in student database")
        
        # Check ALL tables that exist in answer database
        for table_name, answer_count in answer_counts.items():
            if answer_count == -1:
                continue
                
            student_count = student_counts.get(table_name, 0)
            difference = student_count - answer_count
            matches = (student_count == answer_count)
            
            # Determine if this is a business logic table
            is_business_table = table_name in BUSINESS_LOGIC_CHANGES
            expected_increase = BUSINESS_LOGIC_CHANGES.get(table_name, 0)
            
            table_info = {
                'answer_count': answer_count,
                'student_count': student_count,
                'difference': difference,
                'matches': matches,
                'is_business_logic': is_business_table
            }
            
            # Add to all tables
            result['all_tables'][table_name] = table_info
            result['total_tables_compared'] += 1
            
            if matches:
                result['total_tables_matched'] += 1
            else:
                result['all_tables_match'] = False
            
            # Special handling for business logic tables
            if is_business_table:
                business_correct = (difference == expected_increase)
                
                result['business_logic_tables'][table_name] = {
                    'answer_count': answer_count,
                    'student_count': student_count,
                    'expected_increase': expected_increase,
                    'actual_increase': difference,
                    'correct': business_correct
                }
                
                if business_correct:
                    result['business_logic_score'] += 1
                
                logger.debug(f"Business table {table_name}: Answer={answer_count}, "
                            f"Student={student_count}, Expected increase={expected_increase}, "
                            f"Actual={difference}, Correct={business_correct}")
            
            logger.debug(f"Table {table_name}: Answer={answer_count}, Student={student_count}, "
                        f"Difference={difference}, Matches={matches}")
        
        # Business logic is complete if all business tables have correct increases
        result['business_logic_complete'] = (result['business_logic_score'] == result['business_logic_max'])
        
        logger.info(f"Row count analysis complete:")
        logger.info(f"  - Total tables: {result['total_tables_compared']}")
        logger.info(f"  - Tables with matching counts: {result['total_tables_matched']}")
        logger.info(f"  - Business logic score: {result['business_logic_score']}/{result['business_logic_max']}")
        
    except Exception as e:
        logger.error(f"Error checking row counts: {e}")
        result['error'] = str(e)
        result['all_tables_match'] = False
        result['business_logic_complete'] = False
    
    return result

def check_row_count_match(answer_conn, student_conn) -> Dict:
    """
    Check if all tables have matching row counts (indicating complete business logic).
    
    This is a simpler check that just compares total row counts.
    """
    
    result = {
        'tables_compared': {},
        'all_match': True,
        'total_matches': 0,
        'total_tables': 0
    }
    
    try:
        answer_counts = get_all_table_counts(answer_conn)
        student_counts = get_all_table_counts(student_conn)
        
        # Compare all tables that exist in answer
        for table_name, answer_count in answer_counts.items():
            if answer_count == -1:
                continue
                
            student_count = student_counts.get(table_name, 0)
            matches = (student_count == answer_count)
            
            result['tables_compared'][table_name] = {
                'answer_count': answer_count,
                'student_count': student_count,
                'matches': matches,
                'difference': student_count - answer_count
            }
            
            result['total_tables'] += 1
            if matches:
                result['total_matches'] += 1
            else:
                result['all_match'] = False
        
        logger.info(f"Row count comparison: {result['total_matches']}/{result['total_tables']} tables match")
        
    except Exception as e:
        logger.error(f"Error comparing row counts: {e}")
        result['error'] = str(e)
        result['all_match'] = False
    
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
    
    for answer_table, table_data in mapped_tables.items():
        is_business = table_data.get('is_business_table', False)
        exact_match = table_data.get('exact_match', False)
        answer_count = table_data.get('answer_count', 0)
        student_count = table_data.get('student_count', 0)
        difference = table_data.get('difference', 0)
        student_table = table_data.get('student_table', 'N/A')
        
        # Determine status and notes
        if is_business:
            business_status = table_data.get('business_status', 'incorrect')
            expected_increase = table_data.get('expected_increase', 0)
            
            if business_status == 'complete_correct':
                status = 'Hoàn hảo'
                note = f'Dữ liệu và nghiệp vụ đều đúng'
                business_correct = 1
            elif business_status == 'business_logic_only':
                status = 'Nghiệp vụ đúng'
                note = f'Nghiệp vụ đúng (+{expected_increase}), thiếu dữ liệu gốc'
                business_correct = 1
            else:
                status = 'Sai'
                note = f'Chênh lệch {difference}, mong đợi +{expected_increase} (nghiệp vụ)'
                business_correct = 0
        else:
            # Regular data table
            if exact_match:
                status = 'Đúng'
                note = 'Dữ liệu import chính xác'
                business_correct = 0  # Not a business table
            else:
                status = 'Chênh lệch'
                note = f'Chênh lệch {difference}'
                business_correct = 0
        
        csv_rows.append({
            'MSSV': student_id,
            'Tên bảng đáp án': answer_table,
            'Tên bảng sinh viên': student_table,
            'Số dòng đáp án': answer_count,
            'Số dòng sinh viên': student_count,
            'Chênh lệch': difference,
            'Số dòng khớp': 1 if exact_match else 0,
            'Đã nhập đúng nghiệp vụ': business_correct,
            'Là bảng nghiệp vụ': 1 if is_business else 0,
            'Điểm nghiệp vụ': f"{summary.get('business_logic_score', 0)}/{summary.get('business_logic_max', 5)}",
            'Trạng thái': status,
            'Ghi chú': note
        })
    
    return csv_rows
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
            'Điểm nghiệp vụ': '0/0',
            'Trạng thái': 'ERROR',
            'Ghi chú': row_count_results['error']
        })
        return csv_rows
    
    # Get overall status
    business_logic_complete = row_count_results.get('business_logic_complete', False)
    business_score = row_count_results.get('business_logic_score', 0)
    business_max = row_count_results.get('business_logic_max', 0)
    data_import_status = row_count_results.get('data_import_status', 'unknown')
    
    # Process mapped tables
    if 'mapped_tables' in row_count_results:
        for answer_table, table_data in row_count_results['mapped_tables'].items():
            
            is_business_table = table_data.get('is_business_table', False)
            student_table = table_data.get('student_table', 'N/A')
            
            # Determine status and explanation
            if table_data.get('exact_match', False):
                if is_business_table:
                    status = 'Hoàn hảo'
                    explanation = 'Khớp hoàn toàn - đã nhập đúng cả data gốc + nghiệp vụ'
                else:
                    status = 'Khớp'
                    explanation = 'Dữ liệu khớp hoàn toàn'
            else:
                # Has difference - check if it's business logic related
                if is_business_table:
                    business_info = row_count_results.get('business_logic_analysis', {}).get(answer_table, {})
                    business_status = business_info.get('status', 'unknown')
                    expected_increase = table_data.get('expected_increase', 0)
                    actual_diff = table_data.get('difference', 0)
                    
                    if business_status == 'business_logic_only':
                        status = 'Thiếu data gốc'
                        explanation = f'Chênh lệch {actual_diff} đúng bằng nghiệp vụ (+{expected_increase}) - có thể thiếu data gốc'
                    elif business_status == 'incorrect':
                        status = 'Sai'
                        explanation = f'Chênh lệch {actual_diff}, mong đợi +{expected_increase} (nghiệp vụ)'
                    else:
                        status = 'Không rõ'
                        explanation = f'Chênh lệch {actual_diff}'
                else:
                    status = 'Chênh lệch'
                    explanation = f'Chênh lệch {table_data.get("difference", 0)}'
            
            csv_rows.append({
                'MSSV': student_id,
                'Tên bảng đáp án': answer_table,
                'Tên bảng sinh viên': student_table,
                'Số dòng đáp án': table_data['answer_count'],
                'Số dòng sinh viên': table_data['student_count'],
                'Chênh lệch': table_data['difference'],
                'Số dòng khớp': 1 if table_data.get('exact_match', False) else 0,
                'Đã nhập đúng nghiệp vụ': 1 if business_logic_complete else 0,
                'Là bảng nghiệp vụ': 1 if is_business_table else 0,
                'Điểm nghiệp vụ': f"{business_score}/{business_max}",
                'Trạng thái': status,
                'Ghi chú': explanation
            })
    
    return csv_rows
