"""
Gemini API embedding module with fallback support.

This module provides text embedding functionality using Google's Gemini API
with intelligent fallback when API key is not available.
"""

import numpy as np
import pickle
import hashlib
import os
import warnings
from functools import lru_cache
from typing import Optional

from ..utils.constants import API_KEY, MODEL, EMBED_CACHE_FILE
from ..utils.domain_dict import COMMON_SCHEMA_PATTERNS

# Global variables for API state
_API_AVAILABLE = False
_GENAI_MODULE = None

def _initialize_gemini():
    """Initialize Gemini API if available."""
    global _API_AVAILABLE, _GENAI_MODULE
    
    try:
        import google.generativeai as genai
        _GENAI_MODULE = genai
        
        # Check for API key
        api_key = API_KEY or os.getenv("GOOGLE_API_KEY") or os.getenv("GEMINI_API_KEY")
        
        if not api_key:
            warnings.warn(
                "No Gemini API key found. Using fallback embedding method. "
                "Set GOOGLE_API_KEY or GEMINI_API_KEY environment variable for full functionality.",
                UserWarning
            )
            _API_AVAILABLE = False
            return False
            
        genai.configure(api_key=api_key)
        _API_AVAILABLE = True
        print("✅ Gemini API initialized successfully")
        return True
        
    except Exception as e:
        warnings.warn(f"Failed to initialize Gemini API: {e}. Using fallback embedding.", UserWarning)
        _API_AVAILABLE = False
        return False

# Initialize on import
_initialize_gemini()

# Load embedding cache
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

def _fallback_embed(text: str) -> np.ndarray:
    """Fallback embedding method using simple hashing when Gemini API is not available."""
    # Simple hash-based embedding for fallback
    hash_val = hashlib.sha256(text.encode()).hexdigest()
    
    # Convert hex to numbers and create a vector
    vec = np.array([int(hash_val[i:i+2], 16) for i in range(0, min(64, len(hash_val)), 2)], dtype=np.float32)
    
    # Pad or truncate to fixed size (384 dimensions like Gemini)
    target_size = 384
    if len(vec) < target_size:
        vec = np.pad(vec, (0, target_size - len(vec)), mode='constant')
    else:
        vec = vec[:target_size]
    
    # Normalize
    vec = vec / (np.linalg.norm(vec) + 1e-8)
    return vec

@lru_cache(maxsize=None)
def embed(text: str) -> np.ndarray:
    """Embed text into vector with caching and domain context."""
    key = hashlib.sha256(text.encode()).hexdigest()
    if key in CACHE:
        return CACHE[key]
    
    try:
        if _API_AVAILABLE and _GENAI_MODULE:
            # Use Gemini API
            context = _get_domain_context(text)
            result = _GENAI_MODULE.embed_content(
                model=MODEL, 
                content=context,
                task_type="SEMANTIC_SIMILARITY"
            )
            vec = np.array(result['embedding'], dtype=np.float32)
            vec /= np.linalg.norm(vec) + 1e-8
        else:
            # Use fallback method
            vec = _fallback_embed(text)
            
    except Exception as e:
        print(f"Warning: Embedding failed for '{text}': {e}. Using fallback.")
        vec = _fallback_embed(text)
    
    # Cache the result
    CACHE[key] = vec
    try:
        with open(EMBED_CACHE_FILE, 'wb') as f:
            pickle.dump(CACHE, f)
    except Exception:
        pass  # Ignore cache save errors
        
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
