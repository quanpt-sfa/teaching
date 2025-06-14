#!/usr/bin/env python3
"""
Database Schema Grading CLI

Command-line interface for the database schema grading system.
"""

import argparse
import os
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from schema_grader import SchemaGrader
from schema_grader.config import GradingConfig
from schema_grader.db.build_schema import build_schema_dict
from schema_grader.db import connection, schema_reader, restore
from schema_grader.db.clean_data import clean_rows


def load_answer_schema(server: str, user: str, password: str, database: str = "00000001") -> dict:
    """Load answer schema from database."""
    try:
        with connection.open_conn(server, user, password, database) as conn:
            ans_struct = schema_reader.get_table_structures(conn)
            ans_struct = clean_rows(ans_struct)
            ans_pk = schema_reader.get_primary_keys(conn)
            ans_fk = schema_reader.get_foreign_keys_full(conn)
            
        return build_schema_dict(ans_struct, ans_pk, ans_fk)
    except Exception as e:
        print(f"Error loading answer schema: {e}")
        sys.exit(1)


def grade_single_command(args):
    """Handle single file grading command."""
    config = GradingConfig(
        server=args.server,
        user=args.user,
        password=args.password,
        data_folder=args.data_folder,
        output_folder=args.output
    )
    
    # Load answer schema
    answer_schema = load_answer_schema(config.server, config.user, config.password)
    
    # Create grader and process
    grader = SchemaGrader(config)
    result = grader.grade_single(args.backup_file, answer_schema, args.output)
    
    if result:
        print(f"✅ Successfully graded: {args.backup_file}")
        print(f"📊 Schema score: {result.get('schema_score', 'N/A')}")
        print(f"📂 Results saved to: {args.output}")
    else:
        print(f"❌ Failed to grade: {args.backup_file}")


def grade_batch_command(args):
    """Handle batch grading command."""
    config = GradingConfig(
        server=args.server,
        user=args.user,
        password=args.password,
        data_folder=args.data_folder,
        output_folder=args.output
    )
    
    # Load answer schema
    answer_schema = load_answer_schema(config.server, config.user, config.password)
    
    # Create grader and process
    grader = SchemaGrader(config)
    results = grader.grade_batch(args.backup_folder, answer_schema, args.output)
    
    print(f"✅ Graded {len(results)} databases")
    print(f"📂 Results saved to: {args.output}")
    
    # Print summary
    if results:
        scores = [r.get('schema_score', 0) for r in results if r.get('schema_score')]
        if scores:
            print(f"📊 Average schema score: {sum(scores)/len(scores):.2f}")


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Database Schema Grading System",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Grade a single database
  python cli.py single student.bak -o results/
  
  # Grade multiple databases
  python cli.py batch backups/ -o results/
  
  # Custom database connection
  python cli.py single student.bak -s localhost -u sa -p password123
        """)
    
    # Global arguments
    parser.add_argument('--server', '-s', default='localhost',
                       help='SQL Server instance (default: localhost)')
    parser.add_argument('--user', '-u', default='sa',
                       help='Database username (default: sa)')
    parser.add_argument('--password', '-p', default='',
                       help='Database password')
    parser.add_argument('--data-folder', default='C:/temp/',
                       help='Temporary data folder (default: C:/temp/)')
    parser.add_argument('--output', '-o', default='results/',
                       help='Output folder (default: results/)')
    
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Single file grading
    single_parser = subparsers.add_parser('single', help='Grade a single database backup')
    single_parser.add_argument('backup_file', help='Path to .bak file')
    single_parser.set_defaults(func=grade_single_command)
    
    # Batch grading
    batch_parser = subparsers.add_parser('batch', help='Grade multiple database backups')
    batch_parser.add_argument('backup_folder', help='Folder containing .bak files')
    batch_parser.set_defaults(func=grade_batch_command)
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        sys.exit(1)
    
    # Validate paths
    if args.command == 'single' and not os.path.exists(args.backup_file):
        print(f"❌ Backup file not found: {args.backup_file}")
        sys.exit(1)
    
    if args.command == 'batch' and not os.path.exists(args.backup_folder):
        print(f"❌ Backup folder not found: {args.backup_folder}")
        sys.exit(1)
    
    # Execute command
    try:
        args.func(args)
    except Exception as e:
        print(f"❌ Error: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
