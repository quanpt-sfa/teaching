"""Module xử lý so khớp khóa ngoại dựa vào bảng ForeignKeyInfo."""

import pandas as pd
import os
from typing import Dict, List, Tuple
from ..utils.normalizer import canonical
from ..embedding.gemini import embed

def format_fk_string(parent_table: str, ref_table: str, fk_cols: List[str], pk_cols: List[str]) -> str:
    """Tạo chuỗi mô tả khóa ngoại để so sánh.
    
    Args:
        parent_table: Bảng chứa khóa ngoại
        ref_table: Bảng được tham chiếu đến
        fk_cols: Các cột khóa ngoại
        pk_cols: Các cột khóa chính được tham chiếu
    
    Returns:
        str: Chuỗi mô tả mối quan hệ khóa ngoại
    """
    return f"{canonical(parent_table)}({','.join(canonical(c) for c in fk_cols)}) -> {canonical(ref_table)}({','.join(canonical(c) for c in pk_cols)})"

def get_foreign_keys_from_info(conn) -> List[Dict]:
    """Lấy thông tin khóa ngoại từ bảng ForeignKeyInfo.
    
    Args:
        conn: Connection đến database
    
    Returns:
        List[Dict]: Danh sách thông tin khóa ngoại
    """
    try:
        sql = """
            SELECT ParentTable, RefTable, FKColumns, PKColumns 
            FROM ForeignKeyInfo
        """
        cursor = conn.cursor()
        fk_list = []
        for parent, ref, fk_cols, pk_cols in cursor.execute(sql).fetchall():
            if parent and ref and fk_cols and pk_cols:  # Skip if any required field is NULL
                try:
                    fk_list.append({
                        'parent_table': parent,
                        'ref_table': ref,
                        'fk_cols': fk_cols.split(','),
                        'pk_cols': pk_cols.split(',')
                    })
                except:
                    continue  # Skip this FK if parsing fails
                    
        return fk_list
    except Exception as e:
        print(f"Error getting foreign keys from ForeignKeyInfo: {e}")
        return []

def compare_foreign_keys(ans_conn, stu_conn, table_mapping: Dict[str, str], 
                        output_file: str) -> Tuple[List[Dict], float]:
    """So sánh khóa ngoại giữa schema đáp án và sinh viên.
    
    Args:
        ans_conn: Connection đến database đáp án
        stu_conn: Connection đến database sinh viên  
        table_mapping: Dict map từ bảng đáp án sang bảng sinh viên (kết quả stage 1)
        output_file: Đường dẫn file CSV đầu ra
    
    Returns:
        Tuple[List[Dict], float]: (Danh sách kết quả chi tiết, Tỷ lệ match)
    """
    try:
        # Lấy khóa ngoại từ cả hai schema
        ans_fks = get_foreign_keys_from_info(ans_conn)
        if not ans_fks:
            print("Warning: No foreign keys found in answer database")
            return [], 0.0
            
        stu_fks = get_foreign_keys_from_info(stu_conn)
        if not stu_fks:
            print("Warning: No foreign keys found in student database")
            return [], 0.0
        
        # Map tên bảng sinh viên về tên chuẩn
        stu_fks_mapped = []
        for fk in stu_fks:
            parent = table_mapping.get(fk['parent_table'].lower(), fk['parent_table'])
            ref = table_mapping.get(fk['ref_table'].lower(), fk['ref_table'])
            stu_fks_mapped.append({
                'parent_table': parent,
                'ref_table': ref,
                'fk_cols': fk['fk_cols'],
                'pk_cols': fk['pk_cols']
            })
        
        # Tạo chuỗi mô tả cho tất cả khóa ngoại
        ans_strings = [format_fk_string(fk['parent_table'], fk['ref_table'], 
                                      fk['fk_cols'], fk['pk_cols']) 
                      for fk in ans_fks]
        
        stu_strings = [format_fk_string(fk['parent_table'], fk['ref_table'],
                                      fk['fk_cols'], fk['pk_cols'])
                      for fk in stu_fks_mapped]
        
        # Tính ma trận điểm tương đồng
        try:
            ans_embeds = [embed(s) for s in ans_strings]
            stu_embeds = [embed(s) for s in stu_strings]
        except Exception as e:
            print(f"Error generating embeddings: {e}")
            return [], 0.0
        
        # Tạo ma trận similarity
        similarity_matrix = []
        for i, ans_emb in enumerate(ans_embeds):
            row = []
            for j, stu_emb in enumerate(stu_embeds):
                score = ans_emb.dot(stu_emb) / (sum(e*e for e in ans_emb)**0.5 * sum(e*e for e in stu_emb)**0.5)
                row.append(score)
            similarity_matrix.append(row)
            
        # Hungarian algorithm để tìm matching tối ưu
        n = len(ans_fks)
        m = len(stu_fks)
        used_ans = set()  # Đánh dấu FK đáp án đã được match
        used_stu = set()  # Đánh dấu FK sinh viên đã được match
        matches = []      # Danh sách các cặp FK match với nhau
        
        # Sort all possible matches by similarity score
        all_matches = []
        for i in range(n):
            for j in range(m):
                score = similarity_matrix[i][j]
                if score >= 0.85:  # Chỉ xét các cặp có điểm tương đồng đủ cao
                    all_matches.append((score, i, j))
                    
        # Sort by score descending
        all_matches.sort(reverse=True)
        
        # Greedy matching với điều kiện 1-1
        results = []
        total_matches = 0
        
        # Khởi tạo kết quả với tất cả là unmatched
        for i in range(n):
            results.append({
                'answer_fk': ans_strings[i],
                'student_fk': '',
                'similarity': 0.0,
                'is_matched': False
            })
            
        # Match các cặp có điểm cao nhất, đảm bảo 1-1
        for score, i, j in all_matches:
            if i not in used_ans and j not in used_stu:
                used_ans.add(i)
                used_stu.add(j)
                results[i].update({
                    'student_fk': stu_strings[j],
                    'similarity': score,
                    'is_matched': True
                })
                total_matches += 1
            
        # Lưu kết quả ra file CSV
        try:
            if results:
                df = pd.DataFrame(results)
                df.to_csv(output_file, index=False, encoding='utf-8-sig')
        except Exception as e:
            print(f"Warning: Failed to save foreign key results to CSV: {e}")
            
        fk_ratio = total_matches / len(ans_fks) if ans_fks else 0.0
        return results, fk_ratio
        
    except Exception as e:
        print(f"Error comparing foreign keys: {e}")
        return [], 0.0
