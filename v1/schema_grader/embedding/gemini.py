import numpy as np, pickle, hashlib, os
import google.generativeai as genai
from functools import lru_cache
from ..utils.constants import API_KEY, MODEL, EMBED_CACHE_FILE
from ..utils.domain_dict import COMMON_SCHEMA_PATTERNS

genai.configure(api_key=API_KEY)

try:
    with open(EMBED_CACHE_FILE, 'rb') as f:
        CACHE = pickle.load(f)
except Exception:
    CACHE = {}

def _get_domain_context(text: str) -> str:
    """Tạo ngữ cảnh miền cho một từ/cụm từ."""
    # Tìm các pattern tương đương trong từ điển miền
    related_terms = []
    text_lower = text.lower()
    
    for pattern, synonyms in COMMON_SCHEMA_PATTERNS.items():
        if pattern in text_lower or any(s in text_lower for s in synonyms):
            related_terms.append(f"{pattern} = " + " = ".join(synonyms))
    
    domain_context = f"""
    Trong ngữ cảnh kế toán và quản lý tài chính:
    {text}
    
    Các khái niệm tương đương:
    {chr(10).join('- ' + t for t in related_terms)}
    
    Quy tắc chung:
    - Chi tiền = Trả tiền = Thanh toán
    - Chi tiết (CT) = Details
    - Mã = ID = Số
    - Tổng tiền = Thành tiền = Amount
    """
    return domain_context

@lru_cache(maxsize=None)
def embed(text: str) -> np.ndarray:
    """Nhúng văn bản thành vector, có cache kết quả và bổ sung ngữ cảnh miền."""
    key = hashlib.sha256(text.encode()).hexdigest()
    if key in CACHE:
        return CACHE[key]
        
    # Thêm ngữ cảnh miền
    context = _get_domain_context(text)
    
    result = genai.embed_content(model=MODEL, content=context,
                                 task_type="SEMANTIC_SIMILARITY")
    vec = np.array(result['embedding'], dtype=np.float32)
    vec /= np.linalg.norm(vec) + 1e-8
    CACHE[key] = vec
    with open(EMBED_CACHE_FILE, 'wb') as f:
        pickle.dump(CACHE, f)
    return vec

def test_similarity():
    """Hàm test độ tương đồng ngữ nghĩa giữa các cụm từ."""
    pairs = [
        ("chi tiền", "trả tiền"),
        ("phiếu chi", "phiếu trả"),
        ("chi tiết chi tiền", "chi tiết trả tiền"),
        ("CT chi tiền", "CT trả tiền"),
        ("ChiTietChiTien", "ChiTietTraTien")
    ]
    
    for a, b in pairs:
        va = embed(a)
        vb = embed(b)
        sim = np.dot(va, vb)
        print(f"{a} ~ {b}: {sim:.3f}")

if __name__ == "__main__":
    test_similarity()
