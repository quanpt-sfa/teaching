from v1.schema_grader.utils.normalizer import canonical

# Test các cặp cột
pairs = [
    ('PhieuMuaHang', 'phieumuahang'),
    ('MaHangHoa', 'MaHang'),
    ('DonGia', 'Dongia'),
    ('SoLuong', 'Soluong'),
    ('TienThue', 'Tienthue')
]

for col1, col2 in pairs:
    c1 = canonical(col1)
    c2 = canonical(col2)
    equal = c1 == c2
    print(f"{col1} -> '{c1}' | {col2} -> '{c2}' | Equal: {equal}")
