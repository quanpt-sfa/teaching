from rapidfuzz import fuzz
from .normalizer import canonical
from .constants import FUZZY_THRESHOLD

def fuzzy_eq(a: str, b: str, th: int = FUZZY_THRESHOLD):
    """So khớp fuzzy giữa hai chuỗi."""
    ca, cb = canonical(a), canonical(b)
    score_token_set = fuzz.token_set_ratio(ca, cb)
    score_partial   = fuzz.partial_ratio(ca, cb)
    score_ratio     = fuzz.ratio(ca.replace(' ', ''), cb.replace(' ', ''))
    score = max(score_token_set, score_partial, score_ratio)
    return score >= th, score

def _get_abbreviation(text: str) -> str:
    """Tạo từ viết tắt từ một chuỗi.
    Ví dụ: NhaCungCap -> NCC, HangHoa -> HH
    """
    parts = []
    current = []
    
    for c in text:
        if c.isupper() and current:
            parts.append(''.join(current))
            current = [c]
        else:
            current.append(c)
    if current:
        parts.append(''.join(current))
    
    return ''.join(p[0].upper() for p in parts if p)

def smart_token_match(a: str, b: str) -> int:
    """So khớp thông minh giữa hai chuỗi, hỗ trợ viết tắt.
    
    Args:
        a: Chuỗi thứ nhất
        b: Chuỗi thứ hai
    
    Returns:
        int: Score từ 0-100 cho độ tương đồng
    """
    # Chuẩn hóa
    a, b = canonical(a), canonical(b)
    
    # Nếu hai chuỗi giống nhau hoàn toàn
    if a == b:
        return 100

    # So sánh viết tắt
    abbr_a = _get_abbreviation(a)
    abbr_b = _get_abbreviation(b)

    # Nếu một chuỗi là viết tắt của chuỗi kia
    if (len(abbr_a) >= 2 and abbr_a == b) or (len(abbr_b) >= 2 and abbr_b == a):
        return 95
    elif abbr_a == abbr_b and len(abbr_a) >= 2:
        return 90
        
    # Đối với trường hợp không khớp viết tắt, dùng fuzzy matching
    token_set = fuzz.token_set_ratio(a, b)
    partial = fuzz.partial_ratio(a, b)
    ratio = fuzz.ratio(a.replace(' ', ''), b.replace(' ', ''))
    
    return max(token_set, partial, ratio)
