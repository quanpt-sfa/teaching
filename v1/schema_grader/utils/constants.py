"""
Constants and configuration values for the database schema grading system.

This module centralizes all constants, regex patterns, thresholds, and API settings.
"""

import re
import os
from typing import Final

# === Text Processing Patterns ===
RE_CAMEL: Final[re.Pattern] = re.compile(r'([a-z])([A-Z])')
RE_NONAZ: Final[re.Pattern] = re.compile(r'[^a-z0-9\s]')
RE_WS: Final[re.Pattern] = re.compile(r'\s+')
STAGE_RE: Final[re.Pattern] = re.compile(r'stage', re.IGNORECASE)

# === Matching Thresholds ===
FUZZY_THRESHOLD: Final[int] = 80
TABLE_SIMILARITY_THRESHOLD: Final[float] = 0.65
COLUMN_SIMILARITY_THRESHOLD: Final[float] = 0.75
SEMANTIC_SIMILARITY_THRESHOLD: Final[float] = 0.5

# === API Configuration ===
# Try multiple sources for API key with fallback
def _get_api_key() -> str:
    """Get API key from multiple sources with priority order."""
    # Priority 1: Environment variables
    key = os.getenv("GOOGLE_API_KEY") or os.getenv("GEMINI_API_KEY")
    if key:
        return key
    
    # Priority 2: Check .env file
    env_file = os.path.join(os.path.dirname(__file__), '..', '..', '..', '.env')
    if os.path.exists(env_file):
        try:
            with open(env_file, 'r', encoding='utf-8') as f:
                for line in f:
                    if line.startswith('GOOGLE_API_KEY=') or line.startswith('GEMINI_API_KEY='):
                        key = line.split('=', 1)[1].strip().strip('"').strip("'")
                        if key:
                            return key
        except Exception:
            pass
    
    # Priority 3: Legacy hardcoded key (fallback)
    legacy_key = "AIzaSyBb-lZaEpKko9jLgKu7ZHUWssLMOmKyXK4"
    return legacy_key

API_KEY: Final[str] = _get_api_key()
MODEL: Final[str] = "models/text-embedding-004"
EMBED_CACHE_FILE: Final[str] = 'embedding_cache.pkl'

# === Database Settings ===
DEFAULT_ANSWER_DB: Final[str] = "00000001"
DEFAULT_SERVER: Final[str] = "localhost"
DEFAULT_USER: Final[str] = "sa"

# === File Extensions ===
BACKUP_EXTENSIONS: Final[tuple] = ('.bak', '.BAK')
EXPORT_EXTENSIONS: Final[tuple] = ('.csv', '.xlsx')

# === Output Folders ===
DEFAULT_OUTPUT_FOLDER: Final[str] = "results"
DEFAULT_TEMP_FOLDER: Final[str] = "temp"
