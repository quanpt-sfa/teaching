import sys, os
# Add the v1 directory to path so we can import schema_grader
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from tkinter import Tk, filedialog, simpledialog
from schema_grader.db.restore import restore_database
from schema_grader.db.connection import open_conn
from schema_grader.db.schema_reader import get_table_structures, get_primary_keys, get_foreign_keys_full
from schema_grader.db.clean_data import clean_rows
from schema_grader.db.build_schema import build_schema_dict
from schema_grader.grading.pipeline import run_batch

def main():
    root = Tk()
    root.withdraw()
    bak_folder = filedialog.askdirectory(title="Chọn thư mục chứa các file .bak")
    if not bak_folder:
        print("Bạn chưa chọn thư mục. Thoát chương trình.")
        sys.exit(1)
    data_folder = filedialog.askdirectory(title="Chọn thư mục lưu file .mdf/.ldf (DATA)")
    if not data_folder:
        print("Bạn chưa chọn thư mục DATA. Thoát chương trình.")
        sys.exit(1)
    server = simpledialog.askstring("Server", "Nhập tên server:", initialvalue="QUANDESK\\MC22")
    if server is None:
        print("Bạn đã hủy nhập server. Thoát chương trình.")
        sys.exit(1)
    user = simpledialog.askstring("User", "Nhập user:", initialvalue="sa")
    if user is None:
        print("Bạn đã hủy nhập user. Thoát chương trình.")
        sys.exit(1)
    password = simpledialog.askstring("Password", "Nhập password:", show='*', initialvalue="123")
    if password is None:
        print("Bạn đã hủy nhập password. Thoát chương trình.")
        sys.exit(1)
    root.destroy()
    dapan_bak_path = None
    for f in os.listdir(bak_folder):
        if f.lower() == 'dapan.bak':
            dapan_bak_path = os.path.join(bak_folder, f)
            break
    if not dapan_bak_path or not os.path.isfile(dapan_bak_path):
        print("Không tìm thấy file dapan.bak trong thư mục đã chọn. Thoát chương trình.")
        sys.exit(1)
    answer_db = restore_database(dapan_bak_path, server, user, password, data_folder)
    with open_conn(server, user, password, database=answer_db) as conn:
        answer_struct = get_table_structures(conn)
        answer_struct = clean_rows(answer_struct)
        answer_pk = get_primary_keys(conn)
        answer_fk = get_foreign_keys_full(conn)
    
    answer_schema = build_schema_dict(answer_struct, answer_pk, answer_fk)
    out_dir = os.path.join(bak_folder, "pairs_out")
    os.makedirs(out_dir, exist_ok=True)
    
    print("Bắt đầu chấm điểm với kiểm tra row count...")
    results = run_batch(bak_folder, answer_schema, server, user, password, data_folder, out_dir, check_row_counts=True)
    
    print(f"\nHoàn thành! Đã chấm {len(results)} file .bak")
    print(f"Kết quả được lưu trong: {out_dir}")
    print("- schema_grading_results.csv: Tổng kết điểm schema")
    print("- row_count_summary.csv: Thống kê row count và nghiệp vụ")
    print("- [MSSV]_pairs.csv: Chi tiết ghép bảng/cột")
    print("- [MSSV]_fk.csv: Chi tiết khóa ngoại")
    print("- [MSSV]_rowcount.csv: Chi tiết row count từng sinh viên")

if __name__ == "__main__":
    main()
