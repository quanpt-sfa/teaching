"""
Refactored Gemini API embedding module with unified initialization and fallback.
"""

import os
import hashlib
import pickle
import warnings
from functools import lru_cache
from typing import Optional

import numpy as np

from ..utils.constants import API_KEY, MODEL, EMBED_CACHE_FILE
from ..utils.domain_dict import COMMON_SCHEMA_PATTERNS

# State for Gemini API availability
_API_AVAILABLE = False
_GENAI = None


def _load_api_key() -> Optional[str]:
    """Retrieve API key from environment or constants."""
    key = API_KEY or os.getenv("GOOGLE_API_KEY")
    return key.strip() if key else None


def _initialize_api() -> bool:
    """Load genai module, configure and test the Gemini API key."""
    global _API_AVAILABLE, _GENAI
    try:
        import google.generativeai as genai
        _GENAI = genai
    except ImportError:
        warnings.warn("google-generativeai not installed; using fallback embedding.")
        return False

    key = _load_api_key()
    if not key:
        warnings.warn("No Gemini API key found; using fallback embedding.")
        return False

    genai.configure(api_key=key)
    # Single test call to validate key
    try:
        genai.embed_content(model=MODEL, content="test", task_type="SEMANTIC_SIMILARITY")
        _API_AVAILABLE = True
        return True
    except Exception as e:
        warnings.warn(f"Gemini API test failed: {e}; fallback embedding.")
        return False


# Run initialization
_initialize_api()

# Load or create cache
try:
    with open(EMBED_CACHE_FILE, 'rb') as f:
        _CACHE = pickle.load(f)
except Exception:
    _CACHE = {}


def _get_domain_context(text: str) -> str:
    """Build domain-specific context for embedding."""
    text_l = text.lower()
    related = []
    for pat, syns in COMMON_SCHEMA_PATTERNS.items():
        if pat in text_l or any(s in text_l for s in syns):
            related.append(f"{pat}: {', '.join(syns)}")

    lines = [f"- {r}" for r in related]
    context = (
        "Trong ngữ cảnh kế toán và quản lý tài chính:\n"
        f"{text}\n"
        "Các khái niệm tương đương:\n"
        + "\n".join(lines)
    )
    return context


def _fallback_embed(text: str) -> np.ndarray:
    """Hash-based fallback embedding."""
    h = hashlib.sha256(text.encode()).hexdigest()
    arr = np.array([int(h[i:i+2], 16) for i in range(0, min(64, len(h)), 2)], dtype=np.float32)
    if arr.size < 384:
        arr = np.pad(arr, (0, 384 - arr.size), 'constant')
    else:
        arr = arr[:384]
    return arr / (np.linalg.norm(arr) + 1e-8)


@lru_cache(maxsize=None)
def embed(text: str) -> np.ndarray:
    """Embed text via Gemini API or fallback, with caching."""
    key = hashlib.sha256(text.encode()).hexdigest()
    if key in _CACHE:
        return _CACHE[key]

    if _API_AVAILABLE and _GENAI:
        content = _get_domain_context(text)
        try:
            resp = _GENAI.embed_content(model=MODEL, content=content, task_type="SEMANTIC_SIMILARITY")
            vec = np.array(resp['embedding'], dtype=np.float32)
            vec /= (np.linalg.norm(vec) + 1e-8)
        except Exception as e:
            warnings.warn(f"Embedding error: {e}; using fallback.")
            vec = _fallback_embed(text)
    else:
        vec = _fallback_embed(text)

    _CACHE[key] = vec
    try:
        with open(EMBED_CACHE_FILE, 'wb') as f:
            pickle.dump(_CACHE, f)
    except Exception:
        pass
    return vec


def test_similarity():
    """Test semantic similarity between sample phrases."""
    pairs = [
        ("chi tiền", "trả tiền"),
        ("phiếu chi", "phiếu trả"),
        ("chi tiết chi tiền", "chi tiết trả tiền"),
        ("CT chi tiền", "CT trả tiền"),
        ("ChiTietChiTien", "ChiTietTraTien"),
    ]
    for a, b in pairs:
        sim = np.dot(embed(a), embed(b))
        print(f"{a} ~ {b}: {sim:.3f}")

if __name__ == "__main__":
    test_similarity()
