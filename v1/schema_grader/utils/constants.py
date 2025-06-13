import re
import os

# Regex patterns
RE_CAMEL = re.compile(r'([a-z])([A-Z])')
RE_NONAZ = re.compile(r'[^a-z0-9\s]')
RE_WS    = re.compile(r'\s+')
STAGE_RE = re.compile(r'^\d+\.')

# Thresholds
FUZZY_THRESHOLD = 80

# API Settings
API_KEY = os.getenv("GEMINI_API_KEY", "AIzaSyBb-lZaEpKko9jLgKu7ZHUWssLMOmKyXK4")
MODEL = "models/text-embedding-004"
EMBED_CACHE_FILE = 'embedding_cache.pkl'
