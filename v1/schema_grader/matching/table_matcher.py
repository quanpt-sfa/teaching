"""
Table matching module for database schema grading.

This module provides intelligent table matching between answer and student schemas
using column similarity analysis and embedding-based semantic matching.
"""

from typing import Dict, List, Tuple
import numpy as np
from scipy.optimize import linear_sum_assignment

from ..embedding.gemini import embed
from .cosine import cosine_mat
from ..utils.alias_maps import TABLE_ALIAS
from ..utils.normalizer import canonical
from ..utils.fuzzy import smart_token_match


def count_matching_columns(ans_cols: List[Tuple[str, str]], 
                          stu_cols: List[Tuple[str, str]], 
                          match_threshold: int = 70) -> int:
    """Count matching columns between two tables.
    
    Args:
        ans_cols: List of (name, type) tuples from answer table
        stu_cols: List of (name, type) tuples from student table
        match_threshold: Threshold for fuzzy matching (lowered from 80 to 70)
    
    Returns:
        int: Number of matching column pairs
    """
    count = 0
    used_stu_cols = set()
    
    for ac, at in ans_cols:
        for i, (sc, st) in enumerate(stu_cols):
            if i in used_stu_cols:  # Tránh một cột sinh viên match nhiều cột đáp án
                continue
                
            # Kiểm tra tên cột bằng nhiều cách
            smart_score = smart_token_match(ac, sc)
            exact_match = canonical(ac) == canonical(sc) or \
                         canonical(ac).replace(' ', '') == canonical(sc).replace(' ', '') or \
                         ac.lower() == sc.lower()
            
            # Match nếu exact hoặc smart_token_match đủ cao
            if exact_match or smart_score >= match_threshold:
                # Kiểm tra type tương thích (linh hoạt hơn)
                type_compatible = (at.lower() == st.lower() or 
                                 # String types tương thích với nhau
                                 (at.lower() in ('char', 'varchar', 'nvarchar', 'nchar') and 
                                  st.lower() in ('char', 'varchar', 'nvarchar', 'nchar')) or
                                 # Number types tương thích với nhau  
                                 (at.lower() in ('int', 'bigint', 'smallint', 'decimal', 'numeric', 'money', 'real', 'float') and
                                  st.lower() in ('int', 'bigint', 'smallint', 'decimal', 'numeric', 'money', 'real', 'float')) or
                                 # Date types tương thích với nhau
                                 (at.lower() in ('date', 'datetime', 'smalldatetime') and
                                  st.lower() in ('date', 'datetime', 'smalldatetime')))
                
                if type_compatible:
                    count += 1
                    used_stu_cols.add(i)
                    break
    return count

def phase1(ans_schema: Dict[str, Dict], stu_schema: Dict[str, Dict], TBL_TH: float = 0.65) -> Dict[str, str | None]:
    """Phase 1: Ghép bảng dựa trên số cột match và embedding.
    
    Args:
        ans_schema: Schema đáp án {cleaned_ans_table_name: {'original_name': str, 'cols': [], ...}}
        stu_schema: Schema sinh viên {cleaned_stu_table_name: {'original_name': str, 'cols': [], ...}}
        TBL_TH: Ngưỡng cosine để ghép bảng (giảm từ 0.80 xuống 0.65)
    
    Returns:
        dict: Mapping từ cleaned_ans_table_name -> {'student_table': cleaned_stu_table_name or None, 'student_original_name': original_stu_name or None}
    """
    ans_cleaned_names = list(ans_schema.keys())
    stu_cleaned_names = list(stu_schema.keys())
    
    if not ans_cleaned_names or not stu_cleaned_names:
        # Handle empty schemas to prevent errors
        mapping = {}
        for a_tbl_cleaned in ans_cleaned_names:
            mapping[a_tbl_cleaned] = {'student_table': None, 'student_original_name': None}
        return mapping

    # Tạo ma trận số cột match
    col_match_matrix = np.zeros((len(ans_cleaned_names), len(stu_cleaned_names)))
    for i, a_tbl_cleaned in enumerate(ans_cleaned_names):
        for j, s_tbl_cleaned in enumerate(stu_cleaned_names):
            col_match_matrix[i, j] = count_matching_columns(
                ans_schema[a_tbl_cleaned]['cols'],
                stu_schema[s_tbl_cleaned]['cols']
            )
    
    # Tính điểm cosine similarity
    # Ensure there are columns to embed, otherwise use table name only
    vecA_list = []
    for t_cleaned in ans_cleaned_names:
        cols_str = ", ".join(c for c, _ in ans_schema[t_cleaned]['cols'][:30])
        embed_text = f"TABLE {t_cleaned}: {cols_str}" if cols_str else f"TABLE {t_cleaned}"
        vecA_list.append(embed(embed_text))
    vecA = np.stack(vecA_list)

    vecB_list = []
    for t_cleaned in stu_cleaned_names:
        cols_str = ", ".join(c for c, _ in stu_schema[t_cleaned]['cols'][:30])
        embed_text = f"TABLE {t_cleaned}: {cols_str}" if cols_str else f"TABLE {t_cleaned}"
        vecB_list.append(embed(embed_text))
    vecB = np.stack(vecB_list)
        
    sim = cosine_mat(vecA, vecB)
    
    # Tạo ma trận cost tổng hợp:
    # - Ưu tiên số cột match (nhân với 1000 để làm trọng số chính)
    # - Dùng cosine similarity làm tiêu chí phụ
    cost_matrix = -(col_match_matrix * 1000 + sim)
      # Loại bỏ các cặp không đạt ngưỡng tối thiểu
    min_cols = 1  # Ít nhất phải có 1 cột match
    cost_matrix[col_match_matrix < min_cols] = 1e6 # Use a large positive number for invalid assignments
    
    # Tìm matching tối ưu
    r, c = linear_sum_assignment(cost_matrix)
    
    # Xây dựng mapping với logic linh hoạt hơn
    mapping: Dict[str, Dict[str, str | None]] = {}
    # used_stu_cleaned = set() # Not strictly needed with linear_sum_assignment if all stu tables are considered assignable once
    
    matched_stu_indices = set()

    for i, j in zip(r, c):
        ans_tbl_cleaned = ans_cleaned_names[i]
        stu_tbl_cleaned = stu_cleaned_names[j]
        
        # Check if this assignment is valid (cost is not the penalty value)
        if cost_matrix[i, j] < 1e5: # Check against a value slightly less than 1e6
            has_col_match = col_match_matrix[i, j] >= min_cols
            has_high_similarity = sim[i, j] >= TBL_TH
            has_medium_similarity = sim[i, j] >= 0.5  # Ngưỡng thấp hơn
            
            if (has_col_match and has_medium_similarity) or has_high_similarity:
                mapping[ans_tbl_cleaned] = {
                    'student_table': stu_tbl_cleaned,
                    'student_original_name': stu_schema[stu_tbl_cleaned]['original_name']
                }
                matched_stu_indices.add(j)
                print(f"Matched table: {ans_tbl_cleaned} (ans_original: {ans_schema[ans_tbl_cleaned]['original_name']}) -> {stu_tbl_cleaned} (stu_original: {stu_schema[stu_tbl_cleaned]['original_name']}) (cols: {col_match_matrix[i,j]}, sim: {sim[i,j]:.3f})")
            else:
                mapping[ans_tbl_cleaned] = {'student_table': None, 'student_original_name': None}
                print(f"No match for table: {ans_tbl_cleaned} (ans_original: {ans_schema[ans_tbl_cleaned]['original_name']}) (best stu: {stu_tbl_cleaned}, cols={col_match_matrix[i,j]}, sim={sim[i,j]:.3f})")
        else:
            # This was a penalized assignment, so no match
            mapping[ans_tbl_cleaned] = {'student_table': None, 'student_original_name': None}
            # print(f"No valid assignment for table: {ans_tbl_cleaned} (ans_original: {ans_schema[ans_tbl_cleaned]['original_name']})")


    # Đảm bảo tất cả bảng đáp án đều có trong mapping
    for tbl_cleaned in ans_cleaned_names:
        if tbl_cleaned not in mapping:
            mapping[tbl_cleaned] = {'student_table': None, 'student_original_name': None}
            # print(f"Ensuring unassigned answer table {tbl_cleaned} (ans_original: {ans_schema[tbl_cleaned]['original_name']}) is in mapping as None.")
            
    return mapping

# ... (rest of the file, if any, can be added here if needed)
# For example, if there's a phase2 or other functions.
# If not, this is the end of the relevant section.
