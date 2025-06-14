#!/usr/bin/env python3
"""
Test the corrected logic with example data.
"""

import sys
from pathlib import Path

# Add the v1 package to path
sys.path.insert(0, str(Path(__file__).parent / "v1"))

from v1.schema_grader.grading.row_count_checker import format_row_count_results

def test_corrected_logic():
    """Test the corrected logic for business table analysis."""
    
    print("=== Testing Corrected Logic ===")
    
    # Example data matching your CSV results
    test_results = {
        'mapped_tables': {
            'ChiTietMuaHang': {
                'answer_table': 'ChiTietMuaHang',
                'student_table': 'CT_MuaHang',
                'answer_count': 56,
                'student_count': 44,  # -12 difference
                'difference': -12,
                'exact_match': False,
                'is_business_table': True,
                'expected_increase': 1
            },
            'HangHoa': {
                'answer_table': 'HangHoa',
                'student_table': 'HangTonKho',
                'answer_count': 25,
                'student_count': 24,  # -1 difference
                'difference': -1,
                'exact_match': False,
                'is_business_table': True,
                'expected_increase': 1
            },
            'LoaiTien': {
                'answer_table': 'LoaiTien',
                'student_table': 'Tien',
                'answer_count': 3,
                'student_count': 2,  # -1 difference (regular table)
                'difference': -1,
                'exact_match': False,
                'is_business_table': False,
                'expected_increase': 0
            },
            'MuaHang': {
                'answer_table': 'MuaHang',
                'student_table': 'Muahangg',
                'answer_count': 39,
                'student_count': 39,  # Perfect match
                'difference': 0,
                'exact_match': True,
                'is_business_table': True,
                'expected_increase': 1
            },
            'NhaCungCap': {
                'answer_table': 'NhaCungCap',
                'student_table': 'NCC',
                'answer_count': 16,
                'student_count': 15,  # -1 difference
                'difference': -1,
                'exact_match': False,
                'is_business_table': True,
                'expected_increase': 1
            },
            'NhanVien': {
                'answer_table': 'NhanVien',
                'student_table': 'NhanVien',
                'answer_count': 8,
                'student_count': 8,  # Perfect match
                'difference': 0,
                'exact_match': True,
                'is_business_table': True,
                'expected_increase': 1
            }
        },
        'summary': {
            'business_logic_score': 2,  # Only MuaHang and NhanVien perfect
            'business_logic_max': 5,
            'data_import_score': 1,  # Only LoaiTien correct (if it was exact)
            'data_import_max': 1
        }
    }
    
    # Test the formatting
    csv_data = format_row_count_results(test_results, "23701621")
    
    print("\n=== Expected Results with Corrected Logic ===")
    print("| Bảng | Chênh lệch | Đã nhập dữ liệu | Đã nhập nghiệp vụ | Trạng thái | Ghi chú |")
    print("|------|------------|-----------------|-------------------|------------|---------|")
    
    for row in csv_data:
        table = row['Tên bảng đáp án']
        diff = row['Chênh lệch']
        data_ok = row['Đã nhập đúng dữ liệu']
        business_ok = row['Đã nhập đúng nghiệp vụ']
        status = row['Trạng thái']
        note = row['Ghi chú']
        
        print(f"| {table} | {diff} | {data_ok} | {business_ok} | {status} | {note} |")
    
    print("\n=== Analysis ===")
    print("✅ ChiTietMuaHang: -12 → Có vấn đề lớn, không chỉ thiếu nghiệp vụ")
    print("✅ HangHoa: -1 → Dữ liệu đúng, chưa làm nghiệp vụ") 
    print("✅ LoaiTien: -1 → Bảng thường, dữ liệu chưa đầy đủ")
    print("✅ MuaHang: 0 → Hoàn hảo")
    print("✅ NhaCungCap: -1 → Dữ liệu đúng, chưa làm nghiệp vụ")
    print("✅ NhanVien: 0 → Hoàn hảo")

if __name__ == "__main__":
    test_corrected_logic()
