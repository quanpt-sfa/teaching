from ..embedding.gemini import embed
from .normalizer import canonical

def col_vec(col_name: str):
    """Chuyển tên cột thành vector, chỉ dùng tên đã chuẩn hóa"""
    return embed(canonical(col_name))
