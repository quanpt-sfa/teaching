#!/usr/bin/env python3
"""
Row Count Analysis Tool for Database Schema Grading

Analyzes row count differences between student databases and answer key,
specifically checking if students correctly implemented the business logic insertions.
"""

import os
import sys
import pandas as pd
from pathlib import Path
from typing import Dict, List, Tuple, Optional

# Add the v1 package to path
sys.path.insert(0, str(Path(__file__).parent / "v1"))

from v1.schema_grader.db import connection, restore, schema_reader
from v1.schema_grader.config import GradingConfig
from v1.schema_grader.utils.log import get_logger

logger = get_logger(__name__)

class RowCountAnalyzer:
    """Analyzer for comparing row counts between databases."""
    
    def __init__(self, config: GradingConfig):
        """Initialize with grading configuration."""
        self.config = config
        self.answer_row_counts = {}
        
    def get_row_counts(self, server: str, user: str, password: str, database: str) -> Dict[str, int]:
        """Get row counts for all tables in a database."""
        row_counts = {}
        
        try:
            with connection.open_conn(server, user, password, database) as conn:
                # Get all table names
                tables_query = """
                SELECT TABLE_NAME 
                FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_TYPE = 'BASE TABLE'
                ORDER BY TABLE_NAME
                """
                
                cursor = conn.cursor()
                cursor.execute(tables_query)
                tables = [row[0] for row in cursor.fetchall()]
                
                # Get row count for each table
                for table in tables:
                    try:
                        count_query = f"SELECT COUNT(*) FROM [{table}]"
                        cursor.execute(count_query)
                        count = cursor.fetchone()[0]
                        row_counts[table] = count
                        logger.debug(f"Table {table}: {count} rows")
                    except Exception as e:
                        logger.warning(f"Could not count rows in table {table}: {e}")
                        row_counts[table] = -1
                        
        except Exception as e:
            logger.error(f"Error getting row counts from {database}: {e}")
            
        return row_counts
    
    def load_answer_row_counts(self):
        """Load row counts from the answer database."""
        logger.info("Loading answer database row counts...")
        
        try:
            self.answer_row_counts = self.get_row_counts(
                self.config.server,
                self.config.user, 
                self.config.password,
                "00000001"  # Answer database name
            )
            
            logger.info(f"Loaded row counts for {len(self.answer_row_counts)} tables from answer database")
            
            # Log answer row counts for reference
            for table, count in sorted(self.answer_row_counts.items()):
                logger.info(f"Answer - {table}: {count} rows")
                
        except Exception as e:
            logger.error(f"Failed to load answer row counts: {e}")
            raise
    
    def analyze_student_database(self, bak_file: str) -> Dict[str, any]:
        """Analyze a single student database."""
        student_id = Path(bak_file).stem
        logger.info(f"Analyzing student {student_id}...")
        
        result = {
            'MSSV': student_id,
            'tables': {},
            'total_matches': 0,
            'business_logic_correct': True
        }
        
        try:
            # Restore student database
            db_name = f"student_{student_id}"
            logger.info(f"Restoring database {db_name} from {bak_file}")
            
            restore.restore_database(
                self.config.server,
                self.config.user,
                self.config.password,
                bak_file,
                db_name,
                self.config.data_folder
            )
            
            # Get student row counts
            student_row_counts = self.get_row_counts(
                self.config.server,
                self.config.user,
                self.config.password,
                db_name
            )
            
            # Compare with answer
            for table_name, answer_count in self.answer_row_counts.items():
                student_count = student_row_counts.get(table_name, 0)
                matches = (student_count == answer_count)
                
                result['tables'][table_name] = {
                    'answer_count': answer_count,
                    'student_count': student_count,
                    'difference': student_count - answer_count,
                    'matches': matches
                }
                
                if matches:
                    result['total_matches'] += 1
                else:
                    result['business_logic_correct'] = False
                    
                logger.debug(f"{student_id} - {table_name}: Answer={answer_count}, Student={student_count}, Match={matches}")
            
            # Drop student database to clean up
            try:
                with connection.open_conn(self.config.server, self.config.user, self.config.password, "master") as conn:
                    cursor = conn.cursor()
                    cursor.execute(f"ALTER DATABASE [{db_name}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE")
                    cursor.execute(f"DROP DATABASE [{db_name}]")
                    conn.commit()
                    logger.debug(f"Dropped database {db_name}")
            except Exception as e:
                logger.warning(f"Could not drop database {db_name}: {e}")
                
        except Exception as e:
            logger.error(f"Error analyzing student {student_id}: {e}")
            result['error'] = str(e)
            
        return result
    
    def generate_report(self, results: List[Dict], output_file: str = "row_count_analysis.csv"):
        """Generate detailed report of row count analysis."""
        
        # Prepare data for CSV
        csv_data = []
        
        for result in results:
            student_id = result['MSSV']
            
            if 'error' in result:
                # Add error row
                csv_data.append({
                    'MSSV': student_id,
                    'Tên bảng': 'ERROR',
                    'Số dòng đáp án': 0,
                    'Số dòng sinh viên': 0,
                    'Chênh lệch': 0,
                    'Số dòng khớp': 0,
                    'Đã nhập đúng nghiệp vụ': 0,
                    'Ghi chú': result['error']
                })
            else:
                # Add rows for each table
                for table_name, table_data in result['tables'].items():
                    csv_data.append({
                        'MSSV': student_id,
                        'Tên bảng': table_name,
                        'Số dòng đáp án': table_data['answer_count'],
                        'Số dòng sinh viên': table_data['student_count'],
                        'Chênh lệch': table_data['difference'],
                        'Số dòng khớp': 1 if table_data['matches'] else 0,
                        'Đã nhập đúng nghiệp vụ': 1 if result['business_logic_correct'] else 0,
                        'Ghi chú': 'OK' if table_data['matches'] else f'Chênh lệch {table_data["difference"]}'
                    })
        
        # Create DataFrame and save
        df = pd.DataFrame(csv_data)
        df.to_csv(output_file, index=False, encoding='utf-8-sig')
        
        logger.info(f"Report saved to {output_file}")
        
        # Print summary
        self._print_summary(results)
        
        return output_file
    
    def _print_summary(self, results: List[Dict]):
        """Print summary statistics."""
        print("\n" + "="*80)
        print("TỔNG KẾT PHÂN TÍCH ROW COUNT")
        print("="*80)
        
        total_students = len(results)
        successful_students = len([r for r in results if 'error' not in r])
        business_logic_correct = len([r for r in results if r.get('business_logic_correct', False)])
        
        print(f"Tổng số sinh viên: {total_students}")
        print(f"Phân tích thành công: {successful_students}")
        print(f"Nhập đúng nghiệp vụ: {business_logic_correct}")
        print(f"Tỷ lệ thành công: {business_logic_correct/total_students*100:.1f}%")
        
        if self.answer_row_counts:
            print(f"\nSố lượng bảng trong đáp án: {len(self.answer_row_counts)}")
            print("\nSố dòng trong từng bảng đáp án:")
            for table, count in sorted(self.answer_row_counts.items()):
                print(f"  {table}: {count:,} dòng")
        
        print("\n" + "="*80)

def main():
    """Main function to run row count analysis."""
    
    print("=== DATABASE ROW COUNT ANALYZER ===")
    print()
    
    # Get configuration
    from tkinter import Tk, filedialog, simpledialog
    
    root = Tk()
    root.withdraw()
    
    # Select backup folder
    bak_folder = filedialog.askdirectory(title="Chọn thư mục chứa các file .bak sinh viên")
    if not bak_folder:
        print("Bạn chưa chọn thư mục. Thoát chương trình.")
        sys.exit(1)
    
    # Select data folder
    data_folder = filedialog.askdirectory(title="Chọn thư mục lưu file .mdf/.ldf (DATA)")
    if not data_folder:
        print("Bạn chưa chọn thư mục DATA. Thoát chương trình.")
        sys.exit(1)
    
    # Get server info
    server = simpledialog.askstring("Server", "Nhập tên server:", initialvalue="localhost")
    if server is None:
        print("Bạn đã hủy nhập server. Thoát chương trình.")
        sys.exit(1)
        
    user = simpledialog.askstring("User", "Nhập user:", initialvalue="sa")
    if user is None:
        print("Bạn đã hủy nhập user. Thoát chương trình.")
        sys.exit(1)
        
    password = simpledialog.askstring("Password", "Nhập password:", show='*')
    if password is None:
        print("Bạn đã hủy nhập password. Thoát chương trình.")
        sys.exit(1)
    
    root.destroy()
    
    # Setup configuration
    config = GradingConfig(
        server=server,
        user=user,
        password=password,
        data_folder=data_folder,
        output_folder=str(Path(bak_folder).parent / "results")
    )
    
    # Create output folder
    os.makedirs(config.output_folder, exist_ok=True)
    
    # Initialize analyzer
    analyzer = RowCountAnalyzer(config)
    
    try:
        # Load answer database row counts
        analyzer.load_answer_row_counts()
        
        # Find all .bak files (excluding dapan.bak)
        bak_files = []
        for file in os.listdir(bak_folder):
            if file.lower().endswith('.bak') and file.lower() != 'dapan.bak':
                bak_files.append(os.path.join(bak_folder, file))
        
        if not bak_files:
            print("Không tìm thấy file .bak nào trong thư mục đã chọn.")
            sys.exit(1)
        
        print(f"Tìm thấy {len(bak_files)} file .bak để phân tích...")
        
        # Analyze each student database
        results = []
        for i, bak_file in enumerate(sorted(bak_files), 1):
            print(f"[{i}/{len(bak_files)}] Đang phân tích {Path(bak_file).name}...")
            result = analyzer.analyze_student_database(bak_file)
            results.append(result)
        
        # Generate report
        output_file = os.path.join(config.output_folder, "row_count_analysis.csv")
        analyzer.generate_report(results, output_file)
        
        print(f"\n✅ Hoàn thành! Kết quả đã được lưu vào: {output_file}")
        
    except Exception as e:
        logger.error(f"Error in main process: {e}")
        print(f"❌ Lỗi: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
