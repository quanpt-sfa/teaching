import numpy as np
from scipy.optimize import linear_sum_assignment
from ..embedding.gemini import embed
from .cosine import cosine_mat
from ..utils.alias_maps import TABLE_ALIAS
from ..utils.normalizer import canonical
from ..utils.fuzzy import smart_token_match

def count_matching_columns(ans_cols, stu_cols, match_threshold=80):
    """Đếm số cột match giữa hai bảng.
    
    Args:
        ans_cols: List of (name, type) tuples from answer table
        stu_cols: List of (name, type) tuples from student table
        match_threshold: Ngưỡng điểm cho fuzzy matching
    
    Returns:
        int: Số cặp cột match
    """
    count = 0
    for ac, at in ans_cols:
        for sc, st in stu_cols:
            # Kiểm tra tên cột bằng smart_token_match
            if smart_token_match(ac, sc) >= match_threshold:
                # Kiểm tra type tương thích
                if at.lower() == st.lower() or \
                   (at.lower() in ('char', 'varchar', 'nvarchar') and 
                    st.lower() in ('char', 'varchar', 'nvarchar')):
                    count += 1
                    break
    return count

def phase1(ans_schema, stu_schema, TBL_TH=0.80):
    """Phase 1: Ghép bảng dựa trên số cột match và embedding.
    
    Args:
        ans_schema: Schema đáp án 
        stu_schema: Schema sinh viên
        TBL_TH: Ngưỡng cosine để ghép bảng
    
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
    
    # Xây dựng mapping
    mapping = {}
    used_stu = set()
    
    for i, j in zip(r, c):
        if i < len(ans):
            # Chỉ match nếu có ít nhất 1 cột match và similarity đủ cao
            if col_match_matrix[i, j] >= min_cols and sim[i, j] >= TBL_TH:
                mapping[ans[i]] = stu[j]
                used_stu.add(stu[j])
            else:
                mapping[ans[i]] = None
    
    # Đảm bảo tất cả bảng đáp án đều có trong mapping
    for tbl in ans:
        if tbl not in mapping:
            mapping[tbl] = None
            
    return mapping
