import os
import glob
import pandas as pd

def generate_summary_from_csvs(csv_folder: str, output_path: str) -> None:
    """
    Đọc các file CSV *_pairs.csv, *_fk.csv, *_rowcount.csv trong csv_folder
    và tạo bảng tổng hợp với các cột:
      MSSV | Tỷ lệ cột đúng | Tỷ lệ khóa ngoại đúng | Tỷ lệ nhập dữ liệu đúng | Tỷ lệ nghiệp vụ đúng

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

        summary.append({
            "MSSV": student_id,
            "Tỷ lệ cột đúng": round(col_ratio, 4),
            "Tỷ lệ khóa ngoại đúng": round(fk_ratio, 4),
            "Tỷ lệ nhập dữ liệu đúng": round(data_ratio, 4),
            "Tỷ lệ nghiệp vụ đúng": round(biz_ratio, 4),
        })

    # Ghi ra CSV
    summary_df = pd.DataFrame(summary)
    summary_df.to_csv(output_path, index=False, encoding="utf-8-sig")
    print(f"Summary saved to {output_path}")


if __name__ == "__main__":
    import argparse

    p = argparse.ArgumentParser(description="Generate summary table from grading CSVs")
    p.add_argument("csv_folder", help="Folder chứa *_pairs.csv, *_fk.csv, *_rowcount.csv")
    p.add_argument("output_path", help="Đường dẫn file summary CSV đầu ra")
    args = p.parse_args()

    generate_summary_from_csvs(args.csv_folder, args.output_path)
