"""
Configuration module for the database schema grading system.
"""

import os
from dataclasses import dataclass
from typing import Optional
from pathlib import Path

# Try to load .env file if available
try:
    from dotenv import load_dotenv
    env_path = Path(__file__).parent.parent.parent / '.env'
    if env_path.exists():
        load_dotenv(env_path)
        print(f"✅ Loaded environment from {env_path}")
except ImportError:
    pass  # Silently skip if dotenv not available

from .utils.constants import (
    STAGE_RE, FUZZY_THRESHOLD,
    API_KEY, MODEL, EMBED_CACHE_FILE
)
from .utils.domain_dict import ACCOUNTING_TERMS, COMMON_SCHEMA_PATTERNS
from .utils.alias_maps import TABLE_ALIAS, SCHEMA_SYNONYMS

# Re-export ALIAS for backward compatibility
ALIAS = TABLE_ALIAS

@dataclass
class GradingConfig:
    """Configuration class for database grading operations."""
    
    # Database connection settings
    server: str = "localhost"
    user: str = "sa"
    password: str = ""
    data_folder: str = "C:/temp/"
    
    # Grading thresholds
    table_similarity_threshold: float = 0.65
    column_similarity_threshold: float = 0.75
    fuzzy_match_threshold: int = 70
    
    # Output settings
    output_folder: str = "results/"
    export_detailed_results: bool = True
    export_foreign_key_analysis: bool = True
    export_row_count_analysis: bool = True
    
    # AI/ML settings
    use_gemini_api: bool = True
    gemini_api_key: Optional[str] = None
    embedding_cache_enabled: bool = True
    
    def __post_init__(self):
        """Validate configuration after initialization."""
        if self.use_gemini_api and not self.gemini_api_key:
            self.gemini_api_key = API_KEY
            
        # Create output folder if it doesn't exist
        os.makedirs(self.output_folder, exist_ok=True)
        os.makedirs(self.data_folder, exist_ok=True)
    
    @classmethod
    def from_env(cls) -> 'GradingConfig':
        """Create configuration from environment variables."""
        return cls(
            server=os.getenv('DB_SERVER', 'localhost'),
            user=os.getenv('DB_USER', 'sa'),
            password=os.getenv('DB_PASSWORD', ''),
            data_folder=os.getenv('DATA_FOLDER', 'C:/temp/'),
            output_folder=os.getenv('OUTPUT_FOLDER', 'results/'),
            gemini_api_key=os.getenv('GEMINI_API_KEY')
        )
