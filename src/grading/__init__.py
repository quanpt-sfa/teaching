"""
Database Schema Grading System

A comprehensive system for grading database schemas by comparing
student submissions with reference answers using fuzzy matching
and business logic validation.
"""

__version__ = "1.3.2"
__author__ = "Database Grading Team"

# Core components
from .core.grader import SchemaGrader
from .core.pipeline import GradingPipeline

# Database utilities
from .db.connection import DatabaseConnection
from .db.schema_reader import SchemaReader
from .db.schema_builder import SchemaBuilder

# Matching algorithms
from .matching.table_matcher import TableMatcher
from .matching.column_matcher import ColumnMatcher
from .matching.foreign_key_matcher import ForeignKeyMatcher

# Analysis tools
from .analysis.row_count_analyzer import RowCountAnalyzer
from .analysis.business_logic_checker import BusinessLogicChecker

# Utilities
from .utils.config import ConfigManager
from .utils.logger import get_logger

__all__ = [
    # Core
    'SchemaGrader', 'GradingPipeline',
    # Database
    'DatabaseConnection', 'SchemaReader', 'SchemaBuilder',
    # Matching
    'TableMatcher', 'ColumnMatcher', 'ForeignKeyMatcher',
    # Analysis
    'RowCountAnalyzer', 'BusinessLogicChecker',
    # Utilities
    'ConfigManager', 'get_logger'
]
