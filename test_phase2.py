import sys, os
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

from v1.schema_grader.matching.column_matcher import phase2_one

# Test schema
ans_schema = {
    'ChiTietMuaHang': {
        'cols': [
            ('PhieuMuaHang', 'nvarchar'),
            ('MaHangHoa', 'int'),
            ('DonGia', 'decimal'),
            ('SoLuong', 'decimal'),
            ('TienThue', 'decimal')
        ]
    }
}

stu_schema = {
    'CT_MuaHang': {
        'cols': [
            ('SoHD', 'varchar'),
            ('MaHang', 'char'),
            ('Dongia', 'money'),
            ('Soluong', 'real'),
            ('Thanhtien', 'money')
        ]
    }
}

print("Running test...")
results = phase2_one('ChiTietMuaHang', 'CT_MuaHang', ans_schema, stu_schema)

print("\nResults:")
print("AnsTbl\tAnsCol\tAnsType\tStuTbl\tStuCol\tStuType\tCos\tMatch")
for r in results:
    print(f"{r[0]}\t{r[1]}\t{r[2]}\t{r[3]}\t{r[4]}\t{r[5]}\t{r[6]:.6f}\t{r[7]}")
