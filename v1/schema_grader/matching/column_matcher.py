from ..utils.fuzzy import fuzzy_eq
from ..utils.normalizer import canonical
from ..utils.embedding_helper import col_vec
from ..embedding.gemini import embed
import numpy as np
from scipy.optimize import linear_sum_assignment
from .type_check import same_type

CODE_KEYWORDS = ("ma", "code", "id", "sohieu", "phieu", "voucher")
TYPE_FAMILY = {
    'char':'str','varchar':'str','nvarchar':'str',
    'nchar':'str',
    'int':'int','bigint':'int','smallint':'int',
    'decimal':'num','numeric':'num','money':'num','real':'num','float':'num',
    'date':'dt','datetime':'dt','smalldatetime':'dt'
}

def is_code_column(col_name: str) -> bool:
    name = col_name.lower()
    return any(name.startswith(k) or name.endswith(k) for k in CODE_KEYWORDS)

def semantic_similarity_gemini(col1: str, type1: str, col2: str, type2: str) -> float:
    """Sử dụng Gemini API để đánh giá độ tương đồng semantic giữa 2 cột.
    
    Args:
        col1: Tên cột thứ nhất
        type1: Kiểu dữ liệu cột thứ nhất
        col2: Tên cột thứ hai  
        type2: Kiểu dữ liệu cột thứ hai
        
    Returns:
        float: Điểm tương đồng từ 0.0 đến 1.0
    """
    try:
        # Tạo prompt cho Gemini để đánh giá tính tương đồng
        prompt = f"""So sánh 2 cột database sau và cho điểm tương đồng từ 0.0 đến 1.0:

Cột 1: "{col1}" (kiểu: {type1})
Cột 2: "{col2}" (kiểu: {type2})

Xem xét:
- Ý nghĩa semantic (ví dụ: MaHangHoa vs MaHang đều là mã hàng hóa)
- Chức năng tương tự (ví dụ: DonGia vs GiaBan đều là giá tiền)
- Từ viết tắt (ví dụ: PC có thể là PhieuChi)
- Kiểu dữ liệu có phù hợp không

Chỉ trả về 1 số thập phân duy nhất từ 0.0 đến 1.0, không giải thích."""

        # Tạo embedding để tính similarity
        embed1 = embed(f"{col1} {type1}")
        embed2 = embed(f"{col2} {type2}")
        
        # Tính cosine similarity
        dot_product = sum(a * b for a, b in zip(embed1, embed2))
        norm1 = sum(a * a for a in embed1) ** 0.5
        norm2 = sum(b * b for b in embed2) ** 0.5
        
        if norm1 == 0 or norm2 == 0:
            return 0.0
            
        similarity = dot_product / (norm1 * norm2)
        return max(0.0, min(1.0, similarity))  # Clamp về [0,1]
        
    except Exception as e:
        print(f"Error in semantic similarity: {e}")
        return 0.0

def phase2_one(ans_tbl, stu_tbl, ans_schema, stu_schema):
    """Ghép cột cho một cặp bảng đã cố định.
    
    Args:
        ans_tbl: Tên bảng đáp án
        stu_tbl: Tên bảng sinh viên hoặc None
        ans_schema: Schema đáp án
        stu_schema: Schema sinh viên
    
    Returns:
        list: Các dòng kết quả ghép cột
    """
    ans_cols = ans_schema[ans_tbl]['cols']
    if stu_tbl is None:
        return [[ans_tbl, c, d, "—", "—", "—", 0.0, False] for c, d in ans_cols]

    stu_cols = stu_schema[stu_tbl]['cols']
    if not ans_cols or not stu_cols:
        return [[ans_tbl, c, d, stu_tbl, "—", "—", 0.0, False] for c, d in ans_cols]    # Bước 1: name-match trực tiếp  
    matched_idx_stu = set()
    rows = []
    for i, (cA, tA) in enumerate(ans_cols):
        hit = None
        for j, (cS, tS) in enumerate(stu_cols):
            if j in matched_idx_stu:  # đã dùng
                continue
            
            # Kiểm tra exact match với nhiều cách so sánh
            cA_canonical = canonical(cA)
            cS_canonical = canonical(cS)
            
            # So sánh trực tiếp hoặc loại bỏ spaces
            exact_match = (cA_canonical == cS_canonical or 
                          cA_canonical.replace(' ', '') == cS_canonical.replace(' ', '') or
                          cA.lower() == cS.lower())
            
            if exact_match:
                ok = same_type(tA, tS, cA, cS)
                score = 1.0  # Luôn cho điểm tối đa cho exact match
                rows.append([ans_tbl, cA, tA, stu_tbl, cS, tS, score, ok])
                matched_idx_stu.add(j)
                hit = True
                break
        if hit is None:
            rows.append([ans_tbl, cA, tA, None, None, None, None, None])

    # Bước 2: cosine similarity cho các cột còn lại
    needA = [r for r in rows if r[6] is None]
    needB = [(j, *stu_cols[j]) for j in range(len(stu_cols)) if j not in matched_idx_stu]
    
    if needA and needB:
        # Chuyển tên cột thành vectors
        vecA = np.stack([col_vec(r[1]) for r in needA])
        vecB = np.stack([col_vec(c) for _, c, _ in needB])
        
        # Cosine similarity
        cos = vecA @ vecB.T
        cos /= np.linalg.norm(vecA, axis=1)[:, None]
        cos /= np.linalg.norm(vecB, axis=1)[None, :]
        
        # Dùng Hungarian để tìm matching tối ưu
        cost = -cos
        r_idx, c_idx = linear_sum_assignment(cost)        # Cập nhật kết quả với Gemini semantic analysis
        for i,j in zip(r_idx,c_idx):
            if cos[i, j] <= 0:
                continue
            idx_stu, cS, tS = needB[j]
            cA, tA = needA[i][1:3]
            
            # Tính điểm tương đồng cuối cùng
            cosine_score = float(cos[i, j])
            
            # Nếu cosine similarity không rõ ràng (0.5-0.8), dùng Gemini để cải thiện
            if 0.5 <= cosine_score <= 0.8:
                semantic_score = semantic_similarity_gemini(cA, tA, cS, tS)
                # Lấy điểm cao hơn giữa cosine và semantic
                final_score = max(cosine_score, semantic_score)
            else:
                final_score = cosine_score
            
            # Điều kiện matching: score >= 0.75 và same_type
            ok = final_score >= 0.75 and same_type(tA, tS, cA, cS)
            
            rows_id = rows.index(needA[i])
            rows[rows_id] = [ans_tbl, cA, tA, stu_tbl, cS, tS, final_score, ok]

    # Bước 3: đánh dấu các cột chưa ghép được
    for i in range(len(rows)):
        if rows[i][6] is None:
            rows[i] = [ans_tbl, rows[i][1], rows[i][2], stu_tbl, "—", "—", 0.0, False]

    return rows

def match_all_pairs(answer_schema, student_schema, table_pairs):
    """Thực hiện column matching cho tất cả cặp bảng đã mapping"""
    all_rows = []
    for ans_tbl, stu_tbl in table_pairs.items():
        all_rows.extend(phase2_one(ans_tbl, stu_tbl, answer_schema, student_schema))
    return all_rows
