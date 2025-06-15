"""
Table matching module for database schema grading.

This module provides intelligent table matching between answer and student schemas
using column similarity analysis and embedding-based semantic matching.
"""

from typing import Dict, List, Tuple, Optional
import numpy as np
from scipy.optimize import linear_sum_assignment

from ..utils.logger import get_logger

logger = get_logger(__name__)


class TableMatcher:
    """Handles table matching between answer and student schemas."""
    
    def __init__(self, table_threshold: float = 0.65, min_column_matches: int = 1):
        """Initialize the table matcher.
        
        Args:
            table_threshold: Threshold for cosine similarity in table matching
            min_column_matches: Minimum number of column matches required
        """
        self.table_threshold = table_threshold
        self.min_column_matches = min_column_matches
    
    def count_matching_columns(self, 
                             ans_cols: List[Tuple[str, str]], 
                             stu_cols: List[Tuple[str, str]], 
                             match_threshold: int = 70) -> int:
        """Count matching columns between two tables.
        
        Args:
            ans_cols: List of (name, type) tuples from answer table
            stu_cols: List of (name, type) tuples from student table
            match_threshold: Threshold for fuzzy matching
        
        Returns:
            int: Number of matching column pairs
        """
        count = 0
        used_stu_cols = set()
        
        for ac, at in ans_cols:
            for i, (sc, st) in enumerate(stu_cols):
                if i in used_stu_cols:
                    continue
                    
                # Check column name matching using multiple methods
                smart_score = self._smart_token_match(ac, sc)
                exact_match = (
                    self._canonical(ac) == self._canonical(sc) or
                    self._canonical(ac).replace(' ', '') == self._canonical(sc).replace(' ', '') or
                    ac.lower() == sc.lower()
                )
                
                # Match if exact or smart_token_match score is high enough
                if exact_match or smart_score >= match_threshold:
                    # Check type compatibility (flexible)
                    if self._types_compatible(at, st):
                        count += 1
                        used_stu_cols.add(i)
                        break
        return count
    
    def _smart_token_match(self, str1: str, str2: str) -> float:
        """Calculate smart token matching score between two strings."""
        # Simplified implementation - can be enhanced with fuzzy matching
        str1_tokens = set(str1.lower().split())
        str2_tokens = set(str2.lower().split())
        
        if not str1_tokens or not str2_tokens:
            return 0.0
            
        intersection = len(str1_tokens & str2_tokens)
        union = len(str1_tokens | str2_tokens)
        
        return (intersection / union) * 100 if union > 0 else 0.0
    
    def _canonical(self, text: str) -> str:
        """Convert text to canonical form."""
        return text.lower().strip().replace('_', ' ')
    
    def _types_compatible(self, type1: str, type2: str) -> bool:
        """Check if two data types are compatible."""
        t1, t2 = type1.lower(), type2.lower()
        
        if t1 == t2:
            return True
        
        # String types
        string_types = {'char', 'varchar', 'nvarchar', 'nchar', 'text'}
        if t1 in string_types and t2 in string_types:
            return True
        
        # Numeric types
        numeric_types = {'int', 'bigint', 'smallint', 'decimal', 'numeric', 
                        'money', 'real', 'float', 'double'}
        if t1 in numeric_types and t2 in numeric_types:
            return True
        
        # Date types
        date_types = {'date', 'datetime', 'smalldatetime', 'timestamp'}
        if t1 in date_types and t2 in date_types:
            return True
        
        return False
    
    def match_tables(self, 
                    ans_schema: Dict[str, Dict], 
                    stu_schema: Dict[str, Dict]) -> Dict[str, Optional[str]]:
        """Match tables between answer and student schemas.
        
        Args:
            ans_schema: Answer schema dictionary
            stu_schema: Student schema dictionary
        
        Returns:
            Dictionary mapping answer table names to student table names (or None)
        """
        ans_tables = list(ans_schema.keys())
        stu_tables = list(stu_schema.keys())
        
        if not ans_tables or not stu_tables:
            logger.warning("Empty schema detected during table matching")
            return {table: None for table in ans_tables}
        
        # Create column match matrix
        col_match_matrix = np.zeros((len(ans_tables), len(stu_tables)))
        for i, ans_table in enumerate(ans_tables):
            for j, stu_table in enumerate(stu_tables):
                col_match_matrix[i, j] = self.count_matching_columns(
                    ans_schema[ans_table].get('cols', []),
                    stu_schema[stu_table].get('cols', [])
                )
        
        # Calculate cosine similarity (simplified - can be enhanced with embeddings)
        sim_matrix = self._calculate_similarity_matrix(ans_schema, stu_schema)
        
        # Create combined cost matrix
        cost_matrix = -(col_match_matrix * 1000 + sim_matrix)
        
        # Penalize assignments that don't meet minimum requirements
        cost_matrix[col_match_matrix < self.min_column_matches] = 1e6
        
        # Find optimal matching
        row_indices, col_indices = linear_sum_assignment(cost_matrix)
        
        # Build mapping
        mapping = {}
        for i, j in zip(row_indices, col_indices):
            ans_table = ans_tables[i]
            stu_table = stu_tables[j]
            
            if cost_matrix[i, j] < 1e5:  # Valid assignment
                has_col_match = col_match_matrix[i, j] >= self.min_column_matches
                has_high_similarity = sim_matrix[i, j] >= self.table_threshold
                has_medium_similarity = sim_matrix[i, j] >= 0.5
                
                if (has_col_match and has_medium_similarity) or has_high_similarity:
                    mapping[ans_table] = stu_table
                    logger.info(f"Matched table: {ans_table} -> {stu_table} "
                              f"(cols: {col_match_matrix[i,j]}, sim: {sim_matrix[i,j]:.3f})")
                else:
                    mapping[ans_table] = None
                    logger.info(f"No match for table: {ans_table} "
                              f"(best candidate: {stu_table}, cols={col_match_matrix[i,j]}, "
                              f"sim={sim_matrix[i,j]:.3f})")
            else:
                mapping[ans_table] = None
        
        # Ensure all answer tables are in mapping
        for table in ans_tables:
            if table not in mapping:
                mapping[table] = None
        
        return mapping
    
    def _calculate_similarity_matrix(self, 
                                   ans_schema: Dict[str, Dict], 
                                   stu_schema: Dict[str, Dict]) -> np.ndarray:
        """Calculate similarity matrix between answer and student schemas.
        
        This is a simplified implementation. Can be enhanced with proper embeddings.
        """
        ans_tables = list(ans_schema.keys())
        stu_tables = list(stu_schema.keys())
        
        similarity_matrix = np.zeros((len(ans_tables), len(stu_tables)))
        
        for i, ans_table in enumerate(ans_tables):
            for j, stu_table in enumerate(stu_tables):
                # Simple similarity based on table name and column names
                name_sim = self._name_similarity(ans_table, stu_table)
                
                ans_cols = [col[0] for col in ans_schema[ans_table].get('cols', [])]
                stu_cols = [col[0] for col in stu_schema[stu_table].get('cols', [])]
                col_sim = self._column_set_similarity(ans_cols, stu_cols)
                
                # Combine similarities
                similarity_matrix[i, j] = (name_sim + col_sim) / 2
        
        return similarity_matrix
    
    def _name_similarity(self, name1: str, name2: str) -> float:
        """Calculate similarity between two table names."""
        name1_canonical = self._canonical(name1)
        name2_canonical = self._canonical(name2)
        
        if name1_canonical == name2_canonical:
            return 1.0
        
        # Simple token-based similarity
        tokens1 = set(name1_canonical.split())
        tokens2 = set(name2_canonical.split())
        
        if not tokens1 or not tokens2:
            return 0.0
        
        intersection = len(tokens1 & tokens2)
        union = len(tokens1 | tokens2)
        
        return intersection / union if union > 0 else 0.0
    
    def _column_set_similarity(self, cols1: List[str], cols2: List[str]) -> float:
        """Calculate similarity between two sets of column names."""
        if not cols1 or not cols2:
            return 0.0
        
        canonical_cols1 = {self._canonical(col) for col in cols1}
        canonical_cols2 = {self._canonical(col) for col in cols2}
        
        intersection = len(canonical_cols1 & canonical_cols2)
        union = len(canonical_cols1 | canonical_cols2)
        
        return intersection / union if union > 0 else 0.0


# Legacy function for backward compatibility
def phase1(ans_schema: Dict[str, Dict], 
          stu_schema: Dict[str, Dict], 
          TBL_TH: float = 0.65) -> Dict[str, Optional[str]]:
    """Legacy function for table matching - maintained for backward compatibility."""
    matcher = TableMatcher(table_threshold=TBL_TH)
    return matcher.match_tables(ans_schema, stu_schema)
