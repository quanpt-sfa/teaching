import os
import glob
import pandas as pd

def generate_summary_from_csvs(csv_folder: str, output_path: str) -> None:
    """
    Đọc các file CSV *_pairs.csv, *_fk.csv, *_rowcount.csv trong csv_folder
    và tạo bảng tổng hợp với các cột:
    git remote remove origin    git remote remove origin      MSSV | Tỷ lệ cột đúng | Tỷ lệ khóa ngoại đúng | Tỷ lệ nhập dữ liệu đúng | Tỷ lệ nghiệp vụ đúng | Tỷ lệ view đúng

    Kết quả sẽ được ghi ra output_path dưới dạng CSV.
    """
    summary = []

    # Tìm tất cả file *_pairs.csv (module 2)
    for pairs_file in glob.glob(os.path.join(csv_folder, "*_pairs.csv")):
        student_id = os.path.basename(pairs_file).rsplit("_pairs.csv", 1)[0]

        # --- Module 2: cột ---
        df_pairs = pd.read_csv(pairs_file, encoding="utf-8-sig")
        # cột 'Match' (boolean or 0/1) → tỷ lệ True
        col_ratio = df_pairs["Match"].astype(bool).mean() if not df_pairs.empty else 0.0

        # --- Module 3: khóa ngoại ---
        fk_file = os.path.join(csv_folder, f"{student_id}_fk.csv")
        if os.path.exists(fk_file):
            df_fk = pd.read_csv(fk_file, encoding="utf-8-sig")
            fk_ratio = df_fk["is_matched"].astype(bool).mean() if not df_fk.empty else 0.0
        else:
            fk_ratio = 0.0

        # --- Module 4: row count ---
        row_file = os.path.join(csv_folder, f"{student_id}_rowcount.csv")
        if os.path.exists(row_file):
            df_row = pd.read_csv(row_file, encoding="utf-8-sig")
            # cột 'Đã nhập đúng dữ liệu' chứa 'Có'/'Không'
            data_ratio = (df_row["Đã nhập đúng dữ liệu"] == "Có").mean()
            biz_ratio  = (df_row["Đã nhập đúng nghiệp vụ"] == "Có").mean()
        else:
            data_ratio = 0.0
            biz_ratio  = 0.0

        # --- Module 5: view ---
        view_file = os.path.join(csv_folder, f"{student_id}_views.csv")
        if os.path.exists(view_file):
            df_view = pd.read_csv(view_file, encoding="utf-8-sig")
            view_ratio = len(df_view) / 3 if len(df_view) > 0 else 0.0
        else:
            view_ratio = 0.0

        # --- Tính điểm ---
        diem_A1 = col_ratio * 3
        diem_A2 = fk_ratio * 2
        diem_A3 = data_ratio * 1
        diem_B  = view_ratio * 3
        diem_C  = biz_ratio * 1
        tong_diem = diem_A1 + diem_A2 + diem_A3 + diem_B + diem_C
        tong_diem = min(round(tong_diem, 2), 10.0)

        summary.append({
            "MSSV": student_id,
            "Tỷ lệ cột đúng": round(col_ratio, 4),
            "Tỷ lệ khóa ngoại đúng": round(fk_ratio, 4),
            "Tỷ lệ nhập dữ liệu đúng": round(data_ratio, 4),
            "Tỷ lệ nghiệp vụ đúng": round(biz_ratio, 4),
            "Tỷ lệ view đúng": round(view_ratio, 4),
            "Điểm A1": round(diem_A1, 2),
            "Điểm A2": round(diem_A2, 2),
            "Điểm A3": round(diem_A3, 2),
            "Điểm B": round(diem_B, 2),
            "Điểm C": round(diem_C, 2),
            "Tổng điểm": tong_diem,
        })

    # Ghi ra CSV
    summary_df = pd.DataFrame(summary)
    summary_df.to_csv(output_path, index=False, encoding="utf-8-sig")


if __name__ == "__main__":
    import argparse

    p = argparse.ArgumentParser(description="Generate summary table from grading CSVs")
    p.add_argument("csv_folder", help="Folder chứa *_pairs.csv, *_fk.csv, *_rowcount.csv")
    p.add_argument("output_path", help="Đường dẫn file summary CSV đầu ra")
    args = p.parse_args()

    generate_summary_from_csvs(args.csv_folder, args.output_path)
