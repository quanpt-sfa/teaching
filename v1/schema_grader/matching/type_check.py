CODE_KEYWORDS = ("ma", "code", "id", "sohieu", "phieu", "voucher")
TYPE_FAMILY = {
    'char':'str','varchar':'str','nvarchar':'str',
    'nchar':'str',
    'int':'int','bigint':'int','smallint':'int',
    'decimal':'num','numeric':'num','money':'num','real':'num','float':'num',
    'date':'dt','datetime':'dt','smalldatetime':'dt'
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
    - Còn lại: phải cùng họ type
    
    Args:
        atype: Kiểu dữ liệu thứ nhất
        btype: Kiểu dữ liệu thứ hai
        col_a: Tên cột thứ nhất
        col_b: Tên cột thứ hai
        
    Returns:
        bool: True nếu hai kiểu tương thích
    """
    if is_code_column(col_a) and is_code_column(col_b):
        return atype.lower() in ('char','varchar','nvarchar','int','bigint','smallint') \
           and btype.lower() in ('char','varchar','nvarchar','int','bigint','smallint')
    fam_a = TYPE_FAMILY.get(atype.lower(), atype.lower())
    fam_b = TYPE_FAMILY.get(btype.lower(), btype.lower())
    return fam_a == fam_b
