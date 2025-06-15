from typing import Dict, List, Any
import os
import pandas as pd
from .row_count_checker import get_table_row_count

# Naming convention for output CSVs:
# - Table pairs:       {student_id}_pairs.csv
# - Foreign keys:      {student_id}_fk.csv
# - Row counts:        {student_id}_rowcount.csv
# - View matches:      {student_id}_views.csv  # <- this module

def get_views_info(conn) -> List[Dict[str, Any]]:
    """
    Lấy danh sách view trong database với số cột và số dòng.
    Returns: List[Dict] với keys: view_name, num_columns, num_rows
    """
    views = []
    cursor = conn.cursor()
    cursor.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.VIEWS")
    for (view_name,) in cursor.fetchall():
        cursor.execute("SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = ?", (view_name,))
        num_columns = cursor.fetchone()[0]
        try:
            cursor.execute(f"SELECT COUNT(*) FROM [{view_name}]")
            num_rows = cursor.fetchone()[0]
        except Exception:
            num_rows = -1
        views.append({
            "view_name": view_name,
            "num_columns": num_columns,
            "num_rows": num_rows
        })
    return views

def match_views(
    answer_schema: Dict[str, Dict[str, Any]],
    student_schema: Dict[str, Dict[str, Any]],
    ans_conn,
    stu_conn
) -> List[Dict[str, Any]]:
    """
    Ghép các view giữa đáp án và sinh viên dựa trên số cột và số dòng.

    Args:
        answer_schema: dict mapping cleaned view name -> {'original_name': str, 'cols': List[Tuple[col_name, type]]}
        student_schema: tương tự cho sinh viên
        ans_conn: kết nối database đáp án
        stu_conn: kết nối database sinh viên

    Returns:
        List[Dict]: Mỗi dict gồm:
            {
              'view': cleaned view name,
              'ans_cols': int,
              'stu_cols': int,
              'ans_rows': int,
              'stu_rows': int,
              'matched': bool
            }
    """
    results: List[Dict[str, Any]] = []

    for view_name, ans_info in answer_schema.items():
        original_view = ans_info.get('original_name', view_name)
        ans_cols = len(ans_info.get('cols', []))
        ans_rows = get_table_row_count(ans_conn, original_view)

        stu_info = student_schema.get(view_name)
        if stu_info:
            stu_original = stu_info.get('original_name', view_name)
            stu_cols = len(stu_info.get('cols', []))
            stu_rows = get_table_row_count(stu_conn, stu_original)
        else:
            stu_cols = 0
            stu_rows = -1

        matched = (ans_cols == stu_cols) and (ans_rows == stu_rows)
        if matched:
            results.append({
                'view': view_name,
                'ans_cols': ans_cols,
                'stu_cols': stu_cols,
                'ans_rows': ans_rows,
                'stu_rows': stu_rows,
                'matched': matched
            })

    return results


def save_view_matches_to_csv(
    view_results: List[Dict[str, Any]],
    student_id: str,
    out_dir: str
) -> None:
    """
    Xuất kết quả so khớp view ra CSV.

    File name: {student_id}_views.csv

    Args:
        view_results: Kết quả trả về từ match_views
        student_id: MSSV dùng để đặt tên file
        out_dir: Thư mục lưu file CSV
    """
    os.makedirs(out_dir, exist_ok=True)
    # Filter only matched == True
    filtered = [row for row in view_results if row.get('matched') is True]
    columns = ['view', 'ans_cols', 'stu_cols', 'ans_rows', 'stu_rows', 'matched']
    df = pd.DataFrame(filtered, columns=columns)
    output_file = os.path.join(out_dir, f"{student_id}_views.csv")
    # If no rows, still write header
    df.to_csv(output_file, index=False, encoding='utf-8-sig')
    print(f"[DEBUG] save_view_matches_to_csv: Saved {len(df)} matches to {output_file}")
