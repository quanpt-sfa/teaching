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
STAGE_RE: Final[re.Pattern] = re.compile(r'^\d+\.')

# === Matching Thresholds ===
FUZZY_THRESHOLD: Final[int] = 80
TABLE_SIMILARITY_THRESHOLD: Final[float] = 0.65
COLUMN_SIMILARITY_THRESHOLD: Final[float] = 0.75
SEMANTIC_SIMILARITY_THRESHOLD: Final[float] = 0.5

# === API Configuration ===
API_KEY: Final[str] = os.getenv("GEMINI_API_KEY", "")
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
