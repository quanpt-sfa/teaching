"""
Type checking utilities for database column type compatibility.

This module provides functions to determine if two database column types
are compatible for matching purposes.
"""

from typing import Tuple

# Keywords that indicate a column contains codes/IDs
CODE_KEYWORDS = ("ma", "code", "id", "sohieu", "phieu", "voucher")

# Type family mapping for compatibility checking
TYPE_FAMILY = {
    # String types
    'char': 'str', 'varchar': 'str', 'nvarchar': 'str', 'nchar': 'str',
    # Integer types  
    'int': 'int', 'bigint': 'int', 'smallint': 'int',
    # Numeric types
    'decimal': 'num', 'numeric': 'num', 'money': 'num', 'real': 'num', 'float': 'num',
    # Date types
    'date': 'dt', 'datetime': 'dt', 'smalldatetime': 'dt'
}

def is_code_column(col_name: str) -> bool:
    """Kiểm tra xem có phải là cột mã/ID không.
    
    Args:
        col_name: Tên cột cần kiểm tra
    
    Returns:
        bool: True nếu là cột mã/ID
    """
    name = col_name.lower()
    return any(name.startswith(k) or name.endswith(k) for k in CODE_KEYWORDS)

def same_type(atype: str, btype: str, col_a: str, col_b: str) -> bool:
    """Kiểm tra hai kiểu dữ liệu có tương thích không.
    
    - Nếu là cột mã: chấp nhận int/varchar/char
    - Còn lại: linh hoạt hơn, chấp nhận các kiểu trong cùng họ
    
    Args:
        atype: Kiểu dữ liệu thứ nhất
        btype: Kiểu dữ liệu thứ hai
        col_a: Tên cột thứ nhất
        col_b: Tên cột thứ hai
        
    Returns:
        bool: True nếu hai kiểu tương thích
    """
    # Chuẩn hóa tên kiểu
    atype_lower = atype.lower()
    btype_lower = btype.lower()
    
    # Nếu một trong hai là cột mã, chấp nhận string và int
    if is_code_column(col_a) or is_code_column(col_b):
        string_types = ('char','varchar','nvarchar','nchar')
        int_types = ('int','bigint','smallint')
        
        a_is_string = atype_lower in string_types
        a_is_int = atype_lower in int_types
        b_is_string = btype_lower in string_types  
        b_is_int = btype_lower in int_types
        
        return (a_is_string or a_is_int) and (b_is_string or b_is_int)
    
    # Cho các cột khác, chấp nhận cùng họ kiểu dữ liệu
    fam_a = TYPE_FAMILY.get(atype_lower, atype_lower)
    fam_b = TYPE_FAMILY.get(btype_lower, btype_lower)
    return fam_a == fam_b
