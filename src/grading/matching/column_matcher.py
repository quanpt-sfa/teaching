"""
Column matching module for database schema grading.

This module provides intelligent column matching between answer and student schemas
using exact matching, cosine similarity, and semantic analysis.
"""

from typing import List, Dict, Tuple, Optional, Any
import numpy as np
from scipy.optimize import linear_sum_assignment

from ..utils.logger import get_logger

logger = get_logger(__name__)


class ColumnMatcher:
    """Handles column matching between answer and student table schemas."""
    
    def __init__(self, similarity_threshold: float = 0.75):
        """Initialize the column matcher.
        
        Args:
            similarity_threshold: Minimum similarity score for column matching
        """
        self.similarity_threshold = similarity_threshold
    
    def match_columns(self, 
                     ans_table: str,
                     stu_table: Optional[str],
                     ans_schema: Dict[str, Dict],
                     stu_schema: Dict[str, Dict]) -> List[List[Any]]:
        """Match columns for a given table pair.
        
        Args:
            ans_table: Answer table name
            stu_table: Student table name (or None if no match)
            ans_schema: Answer schema dictionary
            stu_schema: Student schema dictionary
        
        Returns:
            List of column matching results
        """
        ans_cols = ans_schema.get(ans_table, {}).get('cols', [])
        
        if stu_table is None:
            return [[ans_table, col, dtype, "—", "—", "—", 0.0, False] 
                   for col, dtype in ans_cols]
        
        stu_cols = stu_schema.get(stu_table, {}).get('cols', [])
        
        if not ans_cols or not stu_cols:
            return [[ans_table, col, dtype, stu_table, "—", "—", 0.0, False] 
                   for col, dtype in ans_cols]
        
        # Phase 1: Exact name matching
        matched_stu_indices = set()
        results = []
        
        for ans_col, ans_type in ans_cols:
            exact_match_found = False
            
            for j, (stu_col, stu_type) in enumerate(stu_cols):
                if j in matched_stu_indices:
                    continue
                
                if self._is_exact_match(ans_col, stu_col):
                    type_match = self._types_compatible(ans_type, stu_type)
                    results.append([
                        ans_table, ans_col, ans_type, 
                        stu_table, stu_col, stu_type, 
                        1.0, type_match
                    ])
                    matched_stu_indices.add(j)
                    exact_match_found = True
                    break
            
            if not exact_match_found:
                results.append([ans_table, ans_col, ans_type, None, None, None, None, None])
        
        # Phase 2: Similarity-based matching for remaining columns
        unmatched_ans = [i for i, result in enumerate(results) if result[6] is None]
        unmatched_stu = [(j, stu_cols[j][0], stu_cols[j][1]) 
                        for j in range(len(stu_cols)) 
                        if j not in matched_stu_indices]
        
        if unmatched_ans and unmatched_stu:
            similarity_matrix = self._calculate_similarity_matrix(
                [results[i][1] for i in unmatched_ans],  # ans column names
                [col_name for _, col_name, _ in unmatched_stu]  # stu column names
            )
            
            # Use Hungarian algorithm for optimal assignment
            cost_matrix = -similarity_matrix
            row_indices, col_indices = linear_sum_assignment(cost_matrix)
            
            for i, j in zip(row_indices, col_indices):
                similarity_score = similarity_matrix[i, j]
                
                if similarity_score > 0:
                    ans_idx = unmatched_ans[i]
                    stu_idx, stu_col, stu_type = unmatched_stu[j]
                    
                    ans_col = results[ans_idx][1]
                    ans_type = results[ans_idx][2]
                    
                    # Enhanced similarity check with semantic analysis if needed
                    final_score = self._enhance_similarity_score(
                        ans_col, ans_type, stu_col, stu_type, similarity_score
                    )
                    
                    type_match = (final_score >= self.similarity_threshold and 
                                self._types_compatible(ans_type, stu_type))
                    
                    results[ans_idx] = [
                        ans_table, ans_col, ans_type,
                        stu_table, stu_col, stu_type,
                        final_score, type_match
                    ]
        
        # Phase 3: Mark unmatched columns
        for i, result in enumerate(results):
            if result[6] is None:
                results[i] = [
                    ans_table, result[1], result[2],
                    stu_table, "—", "—", 0.0, False
                ]
        
        return results
    
    def _is_exact_match(self, col1: str, col2: str) -> bool:
        """Check if two column names are exact matches."""
        col1_canonical = self._canonical(col1)
        col2_canonical = self._canonical(col2)
        
        return (
            col1_canonical == col2_canonical or
            col1_canonical.replace(' ', '') == col2_canonical.replace(' ', '') or
            col1.lower() == col2.lower()
        )
    
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
    
    def _calculate_similarity_matrix(self, 
                                   ans_cols: List[str], 
                                   stu_cols: List[str]) -> np.ndarray:
        """Calculate similarity matrix between column sets."""
        if not ans_cols or not stu_cols:
            return np.zeros((len(ans_cols), len(stu_cols)))
        
        similarity_matrix = np.zeros((len(ans_cols), len(stu_cols)))
        
        for i, ans_col in enumerate(ans_cols):
            for j, stu_col in enumerate(stu_cols):
                similarity_matrix[i, j] = self._column_similarity(ans_col, stu_col)
        
        return similarity_matrix
    
    def _column_similarity(self, col1: str, col2: str) -> float:
        """Calculate similarity between two column names."""
        # Token-based similarity
        tokens1 = set(self._canonical(col1).split())
        tokens2 = set(self._canonical(col2).split())
        
        if not tokens1 or not tokens2:
            return 0.0
        
        intersection = len(tokens1 & tokens2)
        union = len(tokens1 | tokens2)
        
        jaccard_similarity = intersection / union if union > 0 else 0.0
        
        # Simple character-based similarity
        char_similarity = self._character_similarity(col1, col2)
        
        # Combine similarities
        return max(jaccard_similarity, char_similarity)
    
    def _character_similarity(self, str1: str, str2: str) -> float:
        """Calculate character-level similarity between two strings."""
        str1_lower = str1.lower()
        str2_lower = str2.lower()
        
        if str1_lower == str2_lower:
            return 1.0
        
        # Simple Levenshtein-like approach
        max_len = max(len(str1_lower), len(str2_lower))
        if max_len == 0:
            return 1.0
        
        # Count common characters
        common_chars = 0
        for i, char in enumerate(str1_lower):
            if i < len(str2_lower) and char == str2_lower[i]:
                common_chars += 1
        
        return common_chars / max_len
    
    def _enhance_similarity_score(self, 
                                 ans_col: str, ans_type: str,
                                 stu_col: str, stu_type: str,
                                 base_score: float) -> float:
        """Enhance similarity score with additional analysis."""
        # For now, return the base score
        # This can be enhanced with semantic analysis or embeddings
        
        # If score is in uncertain range, apply additional checks
        if 0.5 <= base_score <= 0.8:
            # Check for common patterns
            if self._has_common_patterns(ans_col, stu_col):
                return min(base_score + 0.1, 1.0)
        
        return base_score
    
    def _has_common_patterns(self, col1: str, col2: str) -> bool:
        """Check if two column names have common patterns."""
        # Check for common prefixes/suffixes
        col1_lower = col1.lower()
        col2_lower = col2.lower()
        
        # Common patterns like id, name, date, etc.
        patterns = ['id', 'name', 'date', 'time', 'code', 'number', 'amount']
        
        for pattern in patterns:
            if pattern in col1_lower and pattern in col2_lower:
                return True
        
        return False
    
    def match_all_tables(self, 
                        ans_schema: Dict[str, Dict],
                        stu_schema: Dict[str, Dict],
                        table_mappings: Dict[str, Optional[str]]) -> List[List[Any]]:
        """Match columns for all table pairs.
        
        Args:
            ans_schema: Answer schema dictionary
            stu_schema: Student schema dictionary
            table_mappings: Mapping from answer tables to student tables
        
        Returns:
            List of all column matching results
        """
        all_results = []
        
        for ans_table, stu_table in table_mappings.items():
            results = self.match_columns(ans_table, stu_table, ans_schema, stu_schema)
            all_results.extend(results)
        
        return all_results


# Legacy functions for backward compatibility
def phase2_one(ans_tbl: str, stu_tbl: Optional[str], 
              ans_schema: Dict, stu_schema: Dict) -> List[List[Any]]:
    """Legacy function for column matching - maintained for backward compatibility."""
    matcher = ColumnMatcher()
    return matcher.match_columns(ans_tbl, stu_tbl, ans_schema, stu_schema)


def match_all_pairs(answer_schema: Dict, student_schema: Dict, 
                   table_pairs: Dict[str, Optional[str]]) -> List[List[Any]]:
    """Legacy function for matching all table pairs - maintained for backward compatibility."""
    matcher = ColumnMatcher()
    return matcher.match_all_tables(answer_schema, student_schema, table_pairs)


def semantic_similarity_gemini(col1: str, type1: str, col2: str, type2: str) -> float:
    """Legacy function for semantic similarity - simplified implementation."""
    # Simplified implementation without external API dependency
    # Can be enhanced with proper embedding models
    
    col1_tokens = set(col1.lower().split())
    col2_tokens = set(col2.lower().split())
    
    if not col1_tokens or not col2_tokens:
        return 0.0
    
    intersection = len(col1_tokens & col2_tokens)
    union = len(col1_tokens | col2_tokens)
    
    return intersection / union if union > 0 else 0.0
