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
        
        logger.info(f"Answer database real tables: {sorted(set(answer_real_names.values()))}")
        logger.info(f"Student database real tables: {sorted(set(student_real_names.values()))}")
        
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
