import numpy as np
from scipy.optimize import linear_sum_assignment
from ..embedding.gemini import embed
from .cosine import cosine_mat
from ..utils.alias_maps import TABLE_ALIAS
from ..utils.normalizer import canonical
from ..utils.fuzzy import smart_token_match

def count_matching_columns(ans_cols, stu_cols, match_threshold=70):
    """Đếm số cột match giữa hai bảng.
    
    Args:
        ans_cols: List of (name, type) tuples from answer table
        stu_cols: List of (name, type) tuples from student table
        match_threshold: Ngưỡng điểm cho fuzzy matching (giảm từ 80 xuống 70)
    
    Returns:
        int: Số cặp cột match
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

def phase1(ans_schema, stu_schema, TBL_TH=0.65):
    """Phase 1: Ghép bảng dựa trên số cột match và embedding.
    
    Args:
        ans_schema: Schema đáp án 
        stu_schema: Schema sinh viên
        TBL_TH: Ngưỡng cosine để ghép bảng (giảm từ 0.80 xuống 0.65)
    
    Returns:
        dict: Mapping từ bảng đáp án -> bảng sinh viên hoặc None
    """
    ans = list(ans_schema)
    stu = list(stu_schema)
    
    # Tạo ma trận số cột match
    col_match_matrix = np.zeros((len(ans), len(stu)))
    for i, a_tbl in enumerate(ans):
        for j, s_tbl in enumerate(stu):
            col_match_matrix[i, j] = count_matching_columns(
                ans_schema[a_tbl]['cols'],
                stu_schema[s_tbl]['cols']
            )
    
    # Tính điểm cosine similarity
    vecA = np.stack([
        embed(f"TABLE {t}: " + ", ".join(c for c, _ in ans_schema[t]['cols'][:30]))
        for t in ans
    ])
    vecB = np.stack([
        embed(f"TABLE {t}: " + ", ".join(c for c, _ in stu_schema[t]['cols'][:30]))
        for t in stu
    ])
    sim = cosine_mat(vecA, vecB)
    
    # Tạo ma trận cost tổng hợp:
    # - Ưu tiên số cột match (nhân với 1000 để làm trọng số chính)
    # - Dùng cosine similarity làm tiêu chí phụ
    cost_matrix = -(col_match_matrix * 1000 + sim)
      # Loại bỏ các cặp không đạt ngưỡng tối thiểu
    min_cols = 1  # Ít nhất phải có 1 cột match
    cost_matrix[col_match_matrix < min_cols] = 1e6
    
    # Tìm matching tối ưu
    r, c = linear_sum_assignment(cost_matrix)
    
    # Xây dựng mapping với logic linh hoạt hơn
    mapping = {}
    used_stu = set()
    
    for i, j in zip(r, c):
        if i < len(ans):
            # Giảm yêu cầu: có ít nhất 1 cột match HOẶC similarity cao
            has_col_match = col_match_matrix[i, j] >= min_cols
            has_high_similarity = sim[i, j] >= TBL_TH
            has_medium_similarity = sim[i, j] >= 0.5  # Ngưỡng thấp hơn
            
            # Match nếu:
            # 1. Có ít nhất 1 cột match VÀ similarity >= 0.5, HOẶC
            # 2. Similarity rất cao (>= TBL_TH) dù không có cột match rõ ràng
            if (has_col_match and has_medium_similarity) or has_high_similarity:
                mapping[ans[i]] = stu[j]
                used_stu.add(stu[j])
                print(f"Matched table: {ans[i]} -> {stu[j]} (cols: {col_match_matrix[i,j]}, sim: {sim[i,j]:.3f})")
            else:
                mapping[ans[i]] = None
                print(f"No match for table: {ans[i]} (best: cols={col_match_matrix[i,j]}, sim={sim[i,j]:.3f})")
    
    # Đảm bảo tất cả bảng đáp án đều có trong mapping
    for tbl in ans:
        if tbl not in mapping:
            mapping[tbl] = None
            
    return mapping
