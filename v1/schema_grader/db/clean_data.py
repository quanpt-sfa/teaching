import re
from ..utils.constants import STAGE_RE
from .apply_alias import apply_alias

def clean_rows(raw_rows):
    """Làm sạch dữ liệu các hàng từ DB.
    
    - Loại bỏ bảng stage
    - Xóa tiền tố số
    - Áp dụng alias
    
    Args:
        raw_rows: List các tuple (Table, Column, Type) thô
        
    Returns:
        list: Các tuple đã được làm sạch
    """
    cleaned = []
    for raw_t, c, d in raw_rows:
        if STAGE_RE.match(raw_t) or raw_t.strip().lower().startswith('stage'):
            continue
        t = re.sub(r'^\d+\.\s*', '', raw_t)
        cleaned.append((apply_alias(t), apply_alias(c), d))
    return cleaned
