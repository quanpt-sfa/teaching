# Alias cho các tên bảng và cột
TABLE_ALIAS = {
    'chitien': 'tratien',
    'ct_chitien': 'ct_tratien',
    'chitietchitien': 'chitiettratien',
    'pmh': 'phieumuahang',
    'manv': 'manhanvien',
    'sohd': 'sohoadon',
}

# Synonym pairs cho schema matching
SCHEMA_SYNONYMS = {
    # Map tất cả về dạng chuẩn
    'chi': 'tra',
    'chi tien': 'tra tien',
    'phieu chi': 'phieu tra tien',
    'chitiet': 'chi tiet',
    'ct': 'chi tiet',
    
    # Các dạng viết tắt
    'chitietchitien': 'chitiettratien',
    'ct_chitien': 'chitiettratien',
    'cttratien': 'chitiettratien',
    'cttt': 'chitiettratien',
    
    # Dạng ghép từ
    'chitien': 'tratien',
    'phieuchi': 'phieutratien',
    'phieunhap': 'phieumuahang',
    
    # Mapping 2 chiều
    'tra': 'tra',
    'tratien': 'tratien',
    'chitiettratien': 'chitiettratien',
    'phieutratien': 'phieutratien',
}

def build_bidirectional_synonyms(syn_dict):
    """Tạo synonym map 2 chiều."""
    result = {}
    for k, v in syn_dict.items():
        result[k] = v
        result[v] = v  # Map từ chuẩn về chính nó
    return result
