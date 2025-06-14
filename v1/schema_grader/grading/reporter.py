import pandas as pd
import os

def save_schema_results_csv(results, out_path):
    """Save schema grading results to CSV with enhanced information."""
    
    # Prepare enhanced results with row count information
    enhanced_results = []
    
    for result in results:
        enhanced_result = {
            'db_name': result.get('db_name', ''),
            'schema_score': result.get('schema_score', 0),
        }
          # Add business logic information if available
        if 'business_logic_score' in result:
            enhanced_result['business_logic_score'] = result['business_logic_score']
            enhanced_result['business_logic_max'] = result['business_logic_max']
            enhanced_result['business_logic_complete'] = result['business_logic_complete']
            enhanced_result['business_logic_percentage'] = (
                result['business_logic_score'] / result['business_logic_max'] * 100 
                if result['business_logic_max'] > 0 else 0
            )
        elif 'row_count_results' in result:
            # Extract from row_count_results structure
            row_count_results = result['row_count_results']
            enhanced_result['business_logic_score'] = row_count_results.get('business_logic_score', 0)
            enhanced_result['business_logic_max'] = row_count_results.get('business_logic_max', 0)
            enhanced_result['business_logic_complete'] = row_count_results.get('business_logic_complete', False)
            enhanced_result['business_logic_percentage'] = (
                enhanced_result['business_logic_score'] / enhanced_result['business_logic_max'] * 100 
                if enhanced_result['business_logic_max'] > 0 else 0
            )
            enhanced_result['total_tables_compared'] = row_count_results.get('total_tables_compared', 0)
            enhanced_result['total_tables_matched'] = row_count_results.get('total_tables_matched', 0)
            enhanced_result['all_tables_match'] = row_count_results.get('all_tables_match', False)
        else:
            enhanced_result['business_logic_score'] = 0
            enhanced_result['business_logic_max'] = 0
            enhanced_result['business_logic_complete'] = False
            enhanced_result['business_logic_percentage'] = 0
            enhanced_result['total_tables_compared'] = 0
            enhanced_result['total_tables_matched'] = 0
            enhanced_result['all_tables_match'] = False
        
        # Add foreign key information if available
        if 'fk_ratio' in result:
            enhanced_result['fk_ratio'] = result['fk_ratio']
        else:
            enhanced_result['fk_ratio'] = 0
        
        # Add table results information
        if 'table_results' in result:
            table_results = result['table_results']
            enhanced_result['total_tables'] = len(table_results)
            enhanced_result['matched_tables'] = sum(1 for t in table_results if t.get('matched', False))
            enhanced_result['table_match_ratio'] = (
                enhanced_result['matched_tables'] / enhanced_result['total_tables']
                if enhanced_result['total_tables'] > 0 else 0
            )
        
        enhanced_results.append(enhanced_result)
    
    # Save to CSV
    df = pd.DataFrame(enhanced_results)
    df.to_csv(out_path, index=False, encoding='utf-8-sig')
    
    print(f"Enhanced grading results saved to: {out_path}")

def save_row_count_summary(results, out_dir):
    """Create a summary CSV of all row count results."""
    
    all_row_count_data = []
    
    for result in results:
        if 'row_count_results' not in result:
            continue
            
        db_name = result['db_name']
        row_count_results = result['row_count_results']
          # Process mapped tables from the new analysis
        if 'mapped_tables' in row_count_results:
            business_logic_complete = row_count_results.get('business_logic_complete', False)
            business_score = row_count_results.get('business_logic_score', 0)
            business_max = row_count_results.get('business_logic_max', 0)
            data_import_status = row_count_results.get('data_import_status', 'unknown')
            
            for answer_table, table_data in row_count_results['mapped_tables'].items():
                is_business_table = table_data.get('is_business_table', False)
                student_table = table_data.get('student_table', 'N/A')
                exact_match = table_data.get('exact_match', False)
                
                # Determine status description
                if exact_match:
                    if is_business_table:
                        status_desc = 'Hoàn hảo - khớp hoàn toàn'
                    else:
                        status_desc = 'Khớp hoàn toàn'
                else:
                    if is_business_table:
                        business_info = row_count_results.get('business_logic_analysis', {}).get(answer_table, {})
                        business_status = business_info.get('status', 'unknown')
                        
                        if business_status == 'business_logic_only':
                            status_desc = 'Có nghiệp vụ - thiếu data gốc'
                        elif business_status == 'incorrect':
                            status_desc = 'Nghiệp vụ sai'
                        else:
                            status_desc = 'Chênh lệch'
                    else:
                        status_desc = f'Chênh lệch {table_data["difference"]}'
                
                all_row_count_data.append({
                    'MSSV': db_name,
                    'Tên bảng đáp án': answer_table,
                    'Tên bảng sinh viên': student_table,
                    'Số dòng đáp án': table_data['answer_count'],
                    'Số dòng sinh viên': table_data['student_count'],
                    'Chênh lệch': table_data['difference'],
                    'Số dòng khớp': 1 if exact_match else 0,
                    'Đã nhập đúng nghiệp vụ': 1 if business_logic_complete else 0,
                    'Là bảng nghiệp vụ': 1 if is_business_table else 0,
                    'Điểm nghiệp vụ': f"{business_score}/{business_max}",
                    'Trạng thái data': data_import_status,
                    'Ghi chú': status_desc
                })
    
    if all_row_count_data:
        summary_path = os.path.join(out_dir, "row_count_summary.csv")
        df = pd.DataFrame(all_row_count_data)
        df.to_csv(summary_path, index=False, encoding='utf-8-sig')
        print(f"Row count summary saved to: {summary_path}")
        return summary_path
    
    return None
