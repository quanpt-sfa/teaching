"""
Schema matching module for database grading system.

This module provides intelligent matching capabilities for database schemas,
including table matching, column matching, and foreign key matching.
"""

from .table_matcher import TableMatcher, phase1
from .column_matcher import ColumnMatcher, phase2_one, match_all_pairs
from .foreign_key_matcher import ForeignKeyMatcher, compare_foreign_keys

__all__ = [
    'TableMatcher', 'ColumnMatcher', 'ForeignKeyMatcher',
    'phase1', 'phase2_one', 'match_all_pairs', 'compare_foreign_keys'
]
