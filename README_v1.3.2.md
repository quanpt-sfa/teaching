# Database Schema Grading System v1.3.2

Hệ thống chấm điểm schema cơ sở dữ liệu tự động với khả năng phân tích row count và business logic.

## Tính năng chính v1.3.2

### ✅ Sửa lỗi quan trọng
- **Khắc phục lỗi STAGE_RE**: Sửa regex pattern đã loại bỏ nhầm các bảng sinh viên có prefix số (01.HangTonKho, 07.ChiTien, etc.)
- **Xử lý tên bảng gốc**: Đảm bảo sử dụng tên bảng gốc của sinh viên cho việc truy vấn database
- **Mapping chính xác**: Cải thiện việc lưu trữ và sử dụng tên bảng gốc vs tên đã chuẩn hóa

### 🔧 Cải tiến kỹ thuật
- Thay đổi STAGE_RE từ `^\d+\.` thành `stage` (case-insensitive)
- Thêm fuzzy matching cho tên bảng với case-insensitive và whitespace normalization
- Enhanced debug logging để theo dõi quá trình xử lý tên bảng
- Cải thiện error handling cho PK/FK table mismatches

### 📊 Phân tích Row Count
- Kiểm tra row count cho **TẤT CẢ** các bảng được map (không chỉ business logic tables)
- Phân biệt rõ ràng giữa "đã nhập đúng dữ liệu" và "đã nhập đúng nghiệp vụ"
- Xử lý robust cho các bảng unmapped hoặc lỗi
- Output CSV chi tiết với headers tiếng Việt

### 💼 Business Logic Detection
Tự động phát hiện 5 bảng business logic:
- NhaCungCap (+1 Michael Đẹp trai)
- NhanVien (+1 Mariya Sergienko)  
- HangHoa (+1 Crab Meat)
- MuaHang (+1 Purchase order #71)
- ChiTietMuaHang (+1 Purchase detail)

## Cách sử dụng

```bash
# Chạy grading với row count check
python -m v1.cli.grade_bak D:/ChamBai D:/ChamBai/pairs_out --check-row-counts

# Hoặc sử dụng script wrapper
python row_count_analyzer.py
```

## Output Files

1. **schema_grading_results.csv**: Tổng kết điểm schema
2. **row_count_summary.csv**: Thống kê row count và nghiệp vụ  
3. **[MSSV]_pairs.csv**: Chi tiết ghép bảng/cột
4. **[MSSV]_fk.csv**: Chi tiết khóa ngoại
5. **[MSSV]_rowcount.csv**: Chi tiết row count từng sinh viên

## Row Count Analysis Logic

### Bảng Business Logic
- **Đã nhập đúng dữ liệu**: `student_count == answer_count` (dữ liệu gốc đúng)
- **Đã nhập đúng nghiệp vụ**: `difference == expected_increase` (logic được implement)

### Bảng Regular Data  
- **Đã nhập đúng dữ liệu**: `student_count == answer_count` (import chính xác)

## CSV Columns

| Cột | Mô tả |
|-----|-------|
| MSSV | Mã số sinh viên |
| Tên bảng đáp án | Tên bảng trong đáp án (cleaned) |
| Tên bảng sinh viên | Tên bảng gốc của sinh viên |
| Số dòng đáp án | Row count trong database đáp án |
| Số dòng sinh viên | Row count trong database sinh viên |
| Chênh lệch | student_count - answer_count |
| Đã nhập đúng dữ liệu | Có/Không |
| Đã nhập đúng nghiệp vụ | Có/Không |
| Là bảng nghiệp vụ | Có/Không |
| Điểm nghiệp vụ | 'X/5 (formatted để tránh Excel date conversion) |
| Trạng thái | Mô tả chi tiết |
| Ghi chú | Thông tin bổ sung |

## Cấu trúc thư mục

```
grading/
├── v1/
│   ├── schema_grader/
│   │   ├── db/                    # Database operations
│   │   ├── matching/              # Table & column matching
│   │   ├── grading/              # Scoring & analysis
│   │   ├── utils/                # Utilities & constants
│   │   └── config.py
│   └── cli/                      # Command line interface
├── row_count_analyzer.py         # Main analysis script
├── requirements.txt
└── CHANGELOG.md
```

## Requirements

- Python 3.8+
- SQL Server connection
- Dependencies: pandas, numpy, scipy, pyodbc

## Version History

- **v1.3.2**: Fixed STAGE_RE regex, enhanced table name handling
- **v1.3.1**: Improved row count analysis and error handling  
- **v1.3.0**: Added comprehensive row count checking system
- **v1.2.x**: Enhanced schema matching and foreign key analysis
- **v1.1.x**: Basic schema grading functionality
