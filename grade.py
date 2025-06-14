#!/usr/bin/env python3
"""
Legacy compatibility wrapper for the new schema grading system.

This file maintains backward compatibility with the old accounting_db_grading.py
while using the new refactored codebase.
"""

import os
import sys
from pathlib import Path

# Add the new package to path
sys.path.insert(0, str(Path(__file__).parent / "v1"))

# Import new system
from v1.schema_grader import SchemaGrader
from v1.schema_grader.config import GradingConfig
from v1.schema_grader.db.build_schema import build_schema_dict
from v1.schema_grader.db import connection, schema_reader
from v1.schema_grader.db.clean_data import clean_rows

def main():
    """Main function using the new grading system."""
    
    # Setup configuration
    config = GradingConfig(
        server='localhost',
        user='sa', 
        password='',
        data_folder='C:/temp/',
        output_folder='results/'
    )
    
    print("🚀 Database Schema Grading System v1.2")
    print("=" * 50)
    
    # Get backup folder
    try:
        from tkinter import filedialog
        import tkinter as tk
        
        root = tk.Tk()
        root.withdraw()
        
        bak_folder = filedialog.askdirectory(title="Select folder containing .bak files")
        if not bak_folder:
            print("No folder selected. Exiting...")
            return
            
        root.destroy()
        
    except ImportError:
        print("tkinter not available. Please provide backup folder path:")
        bak_folder = input("Backup folder path: ").strip()
        
        if not bak_folder or not os.path.exists(bak_folder):
            print("Invalid folder path. Exiting...")
            return
    
    print(f"📁 Processing folder: {bak_folder}")
    
    # Load answer schema
    print("📊 Loading answer schema...")
    try:
        with connection.open_conn(config.server, config.user, config.password, "00000001") as conn:
            ans_struct = schema_reader.get_table_structures(conn)
            ans_struct = clean_rows(ans_struct)
            ans_pk = schema_reader.get_primary_keys(conn)
            ans_fk = schema_reader.get_foreign_keys_full(conn)
            
        answer_schema = build_schema_dict(ans_struct, ans_pk, ans_fk)
        print(f"✅ Loaded {len(answer_schema)} tables from answer schema")
        
    except Exception as e:
        print(f"❌ Error loading answer schema: {e}")
        return
    
    # Create grader and process
    grader = SchemaGrader(config)
    
    print("🔄 Starting batch grading...")
    results = grader.grade_batch(bak_folder, answer_schema, config.output_folder)
    
    # Print results summary
    print("\n" + "=" * 50)
    print("📋 GRADING SUMMARY")
    print("=" * 50)
    
    if results:
        print(f"✅ Successfully graded: {len(results)} databases")
        
        # Calculate statistics
        scores = [r.get('schema_score', 0) for r in results if r.get('schema_score') is not None]
        if scores:
            print(f"📊 Average schema score: {sum(scores)/len(scores):.2f}")
            print(f"📈 Highest score: {max(scores):.2f}")
            print(f"📉 Lowest score: {min(scores):.2f}")
        
        print(f"📂 Detailed results saved to: {config.output_folder}")
        print(f"📄 Summary CSV: {config.output_folder}/schema_grading_results.csv")
        
        # List individual results
        print("\n📝 Individual Results:")
        for result in results:
            db_name = result.get('db_name', 'Unknown')
            score = result.get('schema_score', 'N/A')
            print(f"  • {db_name}: {score}")
            
    else:
        print("❌ No databases were successfully graded")
    
    print("\n🎉 Grading completed!")

if __name__ == "__main__":
    main()
