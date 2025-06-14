#!/usr/bin/env python3
"""
Business Logic Check Tool

Specifically checks if students correctly implemented the business logic:
1. Added new supplier "Michael Đẹp trai"
2. Added new employee "Mariya Sergienko" 
3. Added new product "Crab Meat (12 - 4 oz tins)"
4. Created purchase order #71
5. Added purchase order details

Expected row count increases:
- NhaCungCap: +1
- NhanVien: +1
- HangHoa: +1
- MuaHang: +1
- ChiTietMuaHang: +1
"""

import os
import sys
import csv
from pathlib import Path
from typing import Dict, List

# Add the v1 package to path
sys.path.insert(0, str(Path(__file__).parent / "v1"))

from v1.schema_grader.db import connection, restore
from v1.schema_grader.config import GradingConfig
from v1.schema_grader.utils.log import get_logger

logger = get_logger(__name__)

# Expected business logic changes
EXPECTED_CHANGES = {
    'NhaCungCap': 1,      # +1 Michael Đẹp trai
    'NhanVien': 1,        # +1 Mariya Sergienko
    'HangHoa': 1,         # +1 Crab Meat
    'MuaHang': 1,         # +1 Purchase order #71
    'ChiTietMuaHang': 1   # +1 Purchase detail
}

def get_table_row_count(server: str, user: str, password: str, database: str, table: str) -> int:
    """Get row count for a specific table."""
    try:
        with connection.open_conn(server, user, password, database) as conn:
            cursor = conn.cursor()
            cursor.execute(f"SELECT COUNT(*) FROM [{table}]")
            return cursor.fetchone()[0]
    except Exception as e:
        logger.error(f"Error counting rows in {table}: {e}")
        return -1

def check_business_logic_implementation(server: str, user: str, password: str, 
                                      answer_db: str, student_db: str) -> Dict:
    """Check if business logic was correctly implemented."""
    
    result = {
        'tables_checked': {},
        'all_correct': True,
        'total_score': 0,
        'max_score': len(EXPECTED_CHANGES)
    }
    
    for table, expected_increase in EXPECTED_CHANGES.items():
        # Get counts
        answer_count = get_table_row_count(server, user, password, answer_db, table)
        student_count = get_table_row_count(server, user, password, student_db, table)
        
        if answer_count == -1 or student_count == -1:
            result['tables_checked'][table] = {
                'answer_count': answer_count,
                'student_count': student_count,
                'expected_increase': expected_increase,
                'actual_increase': 0,
                'correct': False,
                'error': 'Could not read table'
            }
            result['all_correct'] = False
            continue
        
        actual_increase = student_count - answer_count
        is_correct = (actual_increase == expected_increase)
        
        result['tables_checked'][table] = {
            'answer_count': answer_count,
            'student_count': student_count,
            'expected_increase': expected_increase,
            'actual_increase': actual_increase,
            'correct': is_correct
        }
        
        if is_correct:
            result['total_score'] += 1
        else:
            result['all_correct'] = False
    
    return result

def analyze_student_business_logic(config: GradingConfig, bak_file: str) -> Dict:
    """Analyze business logic implementation for one student."""
    
    student_id = Path(bak_file).stem
    student_db = f"student_{student_id}"
    
    result = {
        'MSSV': student_id,
        'success': False,
        'error': None
    }
    
    try:
        # Restore student database
        logger.info(f"Restoring {student_id} database...")
        restore.restore_database(
            config.server,
            config.user,
            config.password,
            bak_file,
            student_db,
            config.data_folder
        )
        
        # Check business logic
        business_check = check_business_logic_implementation(
            config.server,
            config.user,
            config.password,
            "00000001",  # Answer database
            student_db
        )
        
        result.update(business_check)
        result['success'] = True
        
        # Clean up - drop student database
        try:
            with connection.open_conn(config.server, config.user, config.password, "master") as conn:
                cursor = conn.cursor()
                cursor.execute(f"ALTER DATABASE [{student_db}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE")
                cursor.execute(f"DROP DATABASE [{student_db}]")
                conn.commit()
        except Exception as e:
            logger.warning(f"Could not drop database {student_db}: {e}")
            
    except Exception as e:
        logger.error(f"Error analyzing {student_id}: {e}")
        result['error'] = str(e)
        
    return result

def generate_business_logic_report(results: List[Dict], output_file: str):
    """Generate CSV report for business logic analysis."""
    
    # Prepare CSV data
    csv_data = []
    
    for result in results:
        student_id = result['MSSV']
        
        if not result['success']:
            # Error case
            csv_data.append({
                'MSSV': student_id,
                'Tên bảng': 'ERROR',
                'Số dòng đáp án': 0,
                'Số dòng sinh viên': 0,
                'Tăng mong đợi': 0,
                'Tăng thực tế': 0,
                'Số dòng khớp': 0,
                'Đã nhập đúng nghiệp vụ': 0,
                'Điểm': f"0/{len(EXPECTED_CHANGES)}",
                'Ghi chú': result.get('error', 'Unknown error')
            })
        else:
            # Success case - add row for each table
            all_correct = result.get('all_correct', False)
            score = result.get('total_score', 0)
            max_score = result.get('max_score', len(EXPECTED_CHANGES))
            
            for table, table_data in result['tables_checked'].items():
                csv_data.append({
                    'MSSV': student_id,
                    'Tên bảng': table,
                    'Số dòng đáp án': table_data['answer_count'],
                    'Số dòng sinh viên': table_data['student_count'],
                    'Tăng mong đợi': table_data['expected_increase'],
                    'Tăng thực tế': table_data['actual_increase'],
                    'Số dòng khớp': 1 if table_data['correct'] else 0,
                    'Đã nhập đúng nghiệp vụ': 1 if all_correct else 0,
                    'Điểm': f"{score}/{max_score}",
                    'Ghi chú': 'OK' if table_data['correct'] else f"Tăng {table_data['actual_increase']} thay vì {table_data['expected_increase']}"
                })
    
    # Write CSV
    with open(output_file, 'w', newline='', encoding='utf-8-sig') as f:
        if csv_data:
            writer = csv.DictWriter(f, fieldnames=csv_data[0].keys())
            writer.writeheader()
            writer.writerows(csv_data)
    
    print(f"📊 Báo cáo đã được lưu: {output_file}")
    
    # Print summary
    print_summary(results)

def print_summary(results: List[Dict]):
    """Print summary statistics."""
    
    print("\n" + "="*80)
    print("TỔNG KẾT KIỂM TRA NGHIỆP VỤ")
    print("="*80)
    
    total_students = len(results)
    successful_analysis = len([r for r in results if r['success']])
    perfect_scores = len([r for r in results if r.get('all_correct', False)])
    
    print(f"📋 Tổng số sinh viên: {total_students}")
    print(f"✅ Phân tích thành công: {successful_analysis}")
    print(f"🎯 Làm đúng hoàn toàn: {perfect_scores}")
    print(f"📈 Tỷ lệ thành công: {perfect_scores/total_students*100:.1f}%")
    
    print(f"\n📝 Nghiệp vụ cần kiểm tra:")
    for table, expected in EXPECTED_CHANGES.items():
        print(f"   {table}: +{expected} dòng")
    
    # Score distribution
    score_dist = {}
    for result in results:
        if result['success']:
            score = result.get('total_score', 0)
            max_score = result.get('max_score', len(EXPECTED_CHANGES))
            score_key = f"{score}/{max_score}"
            score_dist[score_key] = score_dist.get(score_key, 0) + 1
    
    if score_dist:
        print(f"\n📊 Phân bố điểm:")
        for score, count in sorted(score_dist.items()):
            print(f"   {score}: {count} sinh viên")
    
    print("="*80)

def main():
    """Main function."""
    
    print("=== KIỂM TRA NGHIỆP VỤ DATABASE ===")
    print("Kiểm tra việc thực hiện nghiệp vụ thêm dữ liệu theo yêu cầu")
    print()
    
    # Get configuration via GUI
    from tkinter import Tk, filedialog, simpledialog
    
    root = Tk()
    root.withdraw()
    
    bak_folder = filedialog.askdirectory(title="Chọn thư mục chứa các file .bak sinh viên")
    if not bak_folder:
        print("❌ Bạn chưa chọn thư mục. Thoát chương trình.")
        sys.exit(1)
    
    data_folder = filedialog.askdirectory(title="Chọn thư mục lưu file .mdf/.ldf (DATA)")
    if not data_folder:
        print("❌ Bạn chưa chọn thư mục DATA. Thoát chương trình.")
        sys.exit(1)
    
    server = simpledialog.askstring("Server", "Nhập tên server:", initialvalue="localhost")
    if server is None:
        print("❌ Bạn đã hủy nhập server. Thoát chương trình.")
        sys.exit(1)
        
    user = simpledialog.askstring("User", "Nhập user:", initialvalue="sa")
    if user is None:
        print("❌ Bạn đã hủy nhập user. Thoát chương trình.")
        sys.exit(1)
        
    password = simpledialog.askstring("Password", "Nhập password:", show='*')
    if password is None:
        print("❌ Bạn đã hủy nhập password. Thoát chương trình.")
        sys.exit(1)
    
    root.destroy()
    
    # Setup
    config = GradingConfig(
        server=server,
        user=user,
        password=password,
        data_folder=data_folder,
        output_folder=str(Path(bak_folder).parent / "results")
    )
    
    os.makedirs(config.output_folder, exist_ok=True)
    
    # Find .bak files
    bak_files = []
    for file in os.listdir(bak_folder):
        if file.lower().endswith('.bak') and file.lower() != 'dapan.bak':
            bak_files.append(os.path.join(bak_folder, file))
    
    if not bak_files:
        print("❌ Không tìm thấy file .bak nào.")
        sys.exit(1)
    
    print(f"🔍 Tìm thấy {len(bak_files)} file .bak để kiểm tra...")
    
    # Analyze each student
    results = []
    for i, bak_file in enumerate(sorted(bak_files), 1):
        student_name = Path(bak_file).stem
        print(f"[{i}/{len(bak_files)}] 🔄 Kiểm tra {student_name}...")
        
        result = analyze_student_business_logic(config, bak_file)
        results.append(result)
        
        # Show quick result
        if result['success']:
            score = result.get('total_score', 0)
            max_score = result.get('max_score', len(EXPECTED_CHANGES))
            print(f"   ✅ Hoàn thành: {score}/{max_score} điểm")
        else:
            print(f"   ❌ Lỗi: {result.get('error', 'Unknown')}")
    
    # Generate report
    output_file = os.path.join(config.output_folder, "business_logic_check.csv")
    generate_business_logic_report(results, output_file)
    
    print(f"\n🎉 Hoàn thành! Kết quả: {output_file}")

if __name__ == "__main__":
    main()
