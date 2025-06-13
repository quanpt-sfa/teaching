from ..utils.alias_maps import TABLE_ALIAS

def apply_alias(name: str) -> str:
    """Thay thế tên bằng alias nếu có trong từ điển TABLE_ALIAS.
    
    Args:
        name: Tên cần kiểm tra alias
    
    Returns:
        str: Tên đã được thay thế bởi alias nếu có, không thì giữ nguyên
    """
    return TABLE_ALIAS.get(name.lower(), name)
