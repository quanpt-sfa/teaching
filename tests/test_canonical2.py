from v1.schema_grader.utils.normalizer import canonical

# Test các cặp cột với logic mới
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
    
    # Các cách so sánh như trong code mới
    exact1 = c1 == c2
    exact2 = c1.replace(' ', '') == c2.replace(' ', '')
    exact3 = col1.lower() == col2.lower()
    
    exact_match = exact1 or exact2 or exact3
    
    print(f"{col1} <-> {col2}")
    print(f"  canonical: '{c1}' vs '{c2}' = {exact1}")
    print(f"  no spaces: '{c1.replace(' ', '')}' vs '{c2.replace(' ', '')}' = {exact2}")
    print(f"  lower: '{col1.lower()}' vs '{col2.lower()}' = {exact3}")
    print(f"  EXACT MATCH: {exact_match}")
    print()
