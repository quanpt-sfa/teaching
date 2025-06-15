"""
Foreign key matching module for database schema grading.

This module handles comparison and matching of foreign key constraints
between answer and student database schemas.
"""

from typing import Dict, List, Tuple, Optional, Any
import pandas as pd
from ..utils.logger import get_logger

logger = get_logger(__name__)


class ForeignKeyMatcher:
    """Handles foreign key matching between answer and student schemas."""
    
    def __init__(self, similarity_threshold: float = 0.85):
        """Initialize the foreign key matcher.
        
        Args:
            similarity_threshold: Minimum similarity score for FK matching
        """
        self.similarity_threshold = similarity_threshold
    
    def get_foreign_keys(self, connection) -> List[Dict[str, Any]]:
        """Extract foreign key information from database.
        
        Args:
            connection: Database connection
        
        Returns:
            List of foreign key dictionaries
        """
        try:
            sql = """
                SELECT ParentTable, RefTable, FKColumns, PKColumns 
                FROM ForeignKeyInfo
            """
            cursor = connection.cursor()
            fk_list = []
            
            for parent, ref, fk_cols, pk_cols in cursor.execute(sql).fetchall():
                if parent and ref and fk_cols and pk_cols:
                    try:
                        fk_list.append({
                            'parent_table': parent,
                            'ref_table': ref,
                            'fk_cols': fk_cols.split(','),
                            'pk_cols': pk_cols.split(',')
                        })
                    except Exception as e:
                        logger.warning(f"Failed to parse FK: {e}")
                        continue
            
            return fk_list
            
        except Exception as e:
            logger.error(f"Error getting foreign keys: {e}")
            return []
    
    def format_fk_string(self, parent_table: str, ref_table: str, 
                        fk_cols: List[str], pk_cols: List[str]) -> str:
        """Create foreign key description string for comparison.
        
        Args:
            parent_table: Table containing foreign key
            ref_table: Referenced table
            fk_cols: Foreign key columns
            pk_cols: Referenced primary key columns
        
        Returns:
            Formatted foreign key description string
        """
        parent_canonical = self._canonical(parent_table)
        ref_canonical = self._canonical(ref_table)
        fk_cols_canonical = ','.join(self._canonical(c) for c in fk_cols)
        pk_cols_canonical = ','.join(self._canonical(c) for c in pk_cols)
        
        return f"{parent_canonical}({fk_cols_canonical}) -> {ref_canonical}({pk_cols_canonical})"
    
    def _canonical(self, text: str) -> str:
        """Convert text to canonical form."""
        return text.lower().strip().replace('_', ' ')
    
    def compare_foreign_keys(self, 
                           ans_connection, 
                           stu_connection,
                           table_mapping: Dict[str, Optional[str]],
                           output_file: Optional[str] = None) -> Tuple[List[Dict], float]:
        """Compare foreign keys between answer and student schemas.
        
        Args:
            ans_connection: Answer database connection
            stu_connection: Student database connection
            table_mapping: Mapping from answer tables to student tables
            output_file: Optional CSV output file path
        
        Returns:
            Tuple of (detailed results list, match ratio)
        """
        try:
            # Get foreign keys from both schemas
            ans_fks = self.get_foreign_keys(ans_connection)
            if not ans_fks:
                logger.warning("No foreign keys found in answer database")
                return [], 0.0
            
            stu_fks = self.get_foreign_keys(stu_connection)
            if not stu_fks:
                logger.warning("No foreign keys found in student database")
                return [], 0.0
            
            # Map student table names using table mapping
            reverse_mapping = {v: k for k, v in table_mapping.items() if v is not None}
            
            stu_fks_mapped = []
            for fk in stu_fks:
                parent = reverse_mapping.get(fk['parent_table'].lower(), fk['parent_table'])
                ref = reverse_mapping.get(fk['ref_table'].lower(), fk['ref_table'])
                stu_fks_mapped.append({
                    'parent_table': parent,
                    'ref_table': ref,
                    'fk_cols': fk['fk_cols'],
                    'pk_cols': fk['pk_cols']
                })
            
            # Create description strings for all foreign keys
            ans_strings = [
                self.format_fk_string(fk['parent_table'], fk['ref_table'], 
                                     fk['fk_cols'], fk['pk_cols'])
                for fk in ans_fks
            ]
            
            stu_strings = [
                self.format_fk_string(fk['parent_table'], fk['ref_table'],
                                     fk['fk_cols'], fk['pk_cols'])
                for fk in stu_fks_mapped
            ]
            
            # Calculate similarity matrix
            similarity_matrix = self._calculate_similarity_matrix(ans_strings, stu_strings)
            
            # Find optimal matching
            results, total_matches = self._find_optimal_matching(
                ans_strings, stu_strings, similarity_matrix
            )
            
            # Save results to CSV if requested
            if output_file and results:
                try:
                    df = pd.DataFrame(results)
                    df.to_csv(output_file, index=False, encoding='utf-8-sig')
                    logger.info(f"Saved foreign key results to {output_file}")
                except Exception as e:
                    logger.warning(f"Failed to save FK results to CSV: {e}")
            
            fk_ratio = total_matches / len(ans_fks) if ans_fks else 0.0
            logger.info(f"Foreign key matching ratio: {fk_ratio:.2%}")
            
            return results, fk_ratio
            
        except Exception as e:
            logger.error(f"Error comparing foreign keys: {e}")
            return [], 0.0
    
    def _calculate_similarity_matrix(self, 
                                   ans_strings: List[str], 
                                   stu_strings: List[str]) -> List[List[float]]:
        """Calculate similarity matrix between FK description strings."""
        similarity_matrix = []
        
        for ans_str in ans_strings:
            row = []
            for stu_str in stu_strings:
                score = self._string_similarity(ans_str, stu_str)
                row.append(score)
            similarity_matrix.append(row)
        
        return similarity_matrix
    
    def _string_similarity(self, str1: str, str2: str) -> float:
        """Calculate similarity between two FK description strings."""
        # Simple token-based similarity
        tokens1 = set(str1.split())
        tokens2 = set(str2.split())
        
        if not tokens1 or not tokens2:
            return 0.0
        
        intersection = len(tokens1 & tokens2)
        union = len(tokens1 | tokens2)
        
        jaccard_similarity = intersection / union if union > 0 else 0.0
        
        # Character-level similarity for exact matches
        if str1 == str2:
            return 1.0
        
        # Enhanced similarity for partial matches
        common_chars = sum(1 for a, b in zip(str1, str2) if a == b)
        max_len = max(len(str1), len(str2))
        char_similarity = common_chars / max_len if max_len > 0 else 0.0
        
        return max(jaccard_similarity, char_similarity)
    
    def _find_optimal_matching(self, 
                             ans_strings: List[str],
                             stu_strings: List[str],
                             similarity_matrix: List[List[float]]) -> Tuple[List[Dict], int]:
        """Find optimal matching between answer and student FK strings."""
        n = len(ans_strings)
        m = len(stu_strings)
        
        # Find all possible matches above threshold
        possible_matches = []
        for i in range(n):
            for j in range(m):
                score = similarity_matrix[i][j]
                if score >= self.similarity_threshold:
                    possible_matches.append((score, i, j))
        
        # Sort by score descending
        possible_matches.sort(reverse=True)
        
        # Greedy matching ensuring 1-1 correspondence
        used_ans = set()
        used_stu = set()
        total_matches = 0
        
        # Initialize results
        results = []
        for i in range(n):
            results.append({
                'answer_fk': ans_strings[i],
                'student_fk': '',
                'similarity': 0.0,
                'is_matched': False
            })
        
        # Apply greedy matching
        for score, i, j in possible_matches:
            if i not in used_ans and j not in used_stu:
                used_ans.add(i)
                used_stu.add(j)
                results[i].update({
                    'student_fk': stu_strings[j],
                    'similarity': score,
                    'is_matched': True
                })
                total_matches += 1
                logger.debug(f"Matched FK: {ans_strings[i]} -> {stu_strings[j]} (score: {score:.3f})")
        
        return results, total_matches


# Legacy function for backward compatibility
def compare_foreign_keys(ans_conn, stu_conn, table_mapping: Dict[str, str], 
                        output_file: str) -> Tuple[List[Dict], float]:
    """Legacy function for FK comparison - maintained for backward compatibility."""
    matcher = ForeignKeyMatcher()
    return matcher.compare_foreign_keys(ans_conn, stu_conn, table_mapping, output_file)


def get_foreign_keys_from_info(conn) -> List[Dict]:
    """Legacy function for getting FK info - maintained for backward compatibility."""
    matcher = ForeignKeyMatcher()
    return matcher.get_foreign_keys(conn)


def format_fk_string(parent_table: str, ref_table: str, 
                    fk_cols: List[str], pk_cols: List[str]) -> str:
    """Legacy function for FK string formatting - maintained for backward compatibility."""
    matcher = ForeignKeyMatcher()
    return matcher.format_fk_string(parent_table, ref_table, fk_cols, pk_cols)
