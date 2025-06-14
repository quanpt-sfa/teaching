"""
Database Schema Grading System v1.2

A comprehensive tool for automated database schema grading with AI-enhanced matching.
"""

from .schema_grader import SchemaGrader
from .schema_grader.config import GradingConfig

__version__ = "1.2.0"
__author__ = "Database Grading Team"
__email__ = "support@dbgrading.com"

# Public API
__all__ = [
    'SchemaGrader',
    'GradingConfig',
]
