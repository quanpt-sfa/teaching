"""
Schema Grader Package

Core functionality for database schema analysis and grading.
"""

from .grading.pipeline import run_for_one_bak, run_batch
from .grading.schema_grader import calc_schema_score
from .matching.table_matcher import phase1
from .matching.column_matcher import phase2_one, match_all_pairs
from .foreign_key.fk_matcher import compare_foreign_keys
from .config import GradingConfig

# Main class for easy usage
class SchemaGrader:
    """Main interface for schema grading operations."""
    
    def __init__(self, config: 'GradingConfig'):
        self.config = config
    
    def grade_single(self, bak_path: str, answer_schema: dict, output_dir: str) -> dict:
        """Grade a single database backup file."""
        return run_for_one_bak(
            bak_path, 
            self.config.server,
            self.config.user, 
            self.config.password,
            self.config.data_folder,
            answer_schema, 
            output_dir
        )
    
    def grade_batch(self, bak_folder: str, answer_schema: dict, output_dir: str) -> list:
        """Grade multiple database backup files."""
        return run_batch(
            bak_folder,
            answer_schema,
            self.config.server,
            self.config.user,
            self.config.password, 
            self.config.data_folder,
            output_dir
        )

__all__ = [
    'SchemaGrader',
    'GradingConfig',
    'run_for_one_bak',
    'run_batch',
    'calc_schema_score',
    'phase1',
    'phase2_one', 
    'match_all_pairs',
    'compare_foreign_keys'
]