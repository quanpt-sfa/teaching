# Từ điển cho miền kế toán/tài chính
ACCOUNTING_TERMS = {
    # Động từ nghiệp vụ
    'chi': 'tra',
    'chi tien': 'tra tien',
    'thanh toan': 'tra tien',
    'mua': 'mua hang',
    'nhap': 'mua hang',
    
    # Danh từ - Tài liệu
    'phieu chi': 'phieu tra tien',
    'chung tu chi': 'phieu tra tien',
    'hoa don': 'phieu mua hang',
    'phieu nhap': 'phieu mua hang',
    'ct': 'chi tiet',
    'chitiet': 'chi tiet',
    
    # Đối tượng
    'ncc': 'nha cung cap',
    'khach hang': 'nha cung cap',
    'supplier': 'nha cung cap',
    'vendor': 'nha cung cap',
    
    # Thuộc tính
    'ma': 'id',
    'so': 'id',
    'so phieu': 'id',
    'ngay': 'date',
    'thoigian': 'date',
    'thoigiantra': 'ngay tra',
    'thoigianmua': 'ngay mua',
    'tongtien': 'thanh tien',
    'tong tien': 'thanh tien',
    'tien': 'thanh tien',
    'sotien': 'thanh tien',
    'gia tri': 'thanh tien',
    'so luong': 'quantity',
    'sl': 'quantity',
    'dvt': 'don vi tinh',
    'don vi': 'don vi tinh',
    'unit': 'don vi tinh',
    
    # Prefix/Suffix phổ biến
    'ma_': 'ma',
    'so_': 'so',
    'ngay_': 'ngay',
    '_id': 'ma',
    '_no': 'so',
    '_date': 'ngay',
    '_qty': 'quantity',
    '_amount': 'thanh tien',
}

# Cụm từ hay gặp trong schema
COMMON_SCHEMA_PATTERNS = {
    # Bảng giao dịch chính
    'chi tien': ['tra tien', 'thanh toan', 'payment'],
    'mua hang': ['nhap hang', 'purchase', 'buying'],
    
    # Bảng chi tiết
    'chi tiet chi tien': ['chi tiet tra tien', 'ct tra tien', 'payment details'],
    'chi tiet mua hang': ['chi tiet nhap hang', 'ct mua hang', 'purchase details'],
    
    # Bảng đối tượng
    'nha cung cap': ['ncc', 'supplier', 'vendor'],
    'nhan vien': ['staff', 'employee', 'personnel'],
    'hang hoa': ['mat hang', 'san pham', 'item', 'product'],
    
    # Bảng phụ trợ
    'loai tien': ['tien te', 'currency', 'money type'],
    'kho': ['hang ton kho', 'warehouse', 'inventory'],
}
