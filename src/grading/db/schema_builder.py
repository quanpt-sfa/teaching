"""
Database schema building utilities for grading system.
"""

from collections import defaultdict
from typing import Dict, List, Any, Tuple
from ..utils.logger import get_logger

logger = get_logger(__name__)


class SchemaBuilder:
    """Builder class for constructing schema dictionaries from database metadata."""
    
    def __init__(self):
        """Initialize schema builder."""
        self.schema = defaultdict(lambda: {
            'original_name': '', 
            'columns': [], 
            'primary_keys': [], 
            'foreign_keys': []
        })
    
    def build_schema_dict(self, table_data_list: List[Dict], 
                         pk_dict: Dict[str, List[str]], 
                         fk_list: List[Dict]) -> Dict[str, Dict]:
        """Build schema dictionary from raw database metadata.
        
        Args:
            table_data_list: List of dicts from get_table_structures, 
                           each {'original_name': str, 'cleaned_name': str, 'column_name': str, 'data_type': str}
            pk_dict: Dict {original_table_name: [primary_key_columns]}
            fk_list: List of foreign key info, where table names are original
        
        Returns:
            dict: Schema with structure {cleaned_table_name: {'original_name': str, 'columns': [], 'primary_keys': [], 'foreign_keys': []}}
        """
        logger.debug(f"Building schema from {len(table_data_list)} table data items...")
        
        # Track original names for debugging
        original_names_seen = set()
        
        # Populate schema with columns and original names, keyed by cleaned names
        for item in table_data_list:
            cleaned_t = item['cleaned_name']
            original_t = item['original_name']
            
            original_names_seen.add(original_t)
            
            # Store original name (first one encountered for a cleaned name)
            if not self.schema[cleaned_t]['original_name']:
                self.schema[cleaned_t]['original_name'] = original_t
            elif self.schema[cleaned_t]['original_name'] != original_t:
                # Handle case where multiple original names map to same cleaned name
                logger.warning(
                    f"Cleaned name '{cleaned_t}' maps to multiple original names: "
                    f"'{self.schema[cleaned_t]['original_name']}' and '{original_t}'. "
                    f"Using the first one."
                )

            self.schema[cleaned_t]['columns'].append((item['column_name'], item['data_type']))

        logger.debug(f"Original table names seen in data: {sorted(original_names_seen)}")
        logger.debug(f"PK dict keys: {sorted(pk_dict.keys())}")
        logger.debug(f"FK parent tables: {sorted(set(fk.get('parent_table', fk.get('parent_tbl', '')) for fk in fk_list))}")
        
        # Add primary keys
        self._add_primary_keys(pk_dict)
        
        # Add foreign keys
        self._add_foreign_keys(fk_list)
        
        return dict(self.schema)
    
    def _find_cleaned_name_for_original(self, original_name: str) -> str:
        """Find the cleaned name corresponding to an original table name.
        
        Args:
            original_name: Original table name from database
            
        Returns:
            str: Cleaned table name, or None if not found
        """
        # Try exact match first
        for cleaned_name, data in self.schema.items():
            if data['original_name'] == original_name:
                return cleaned_name
        
        # Try case-insensitive and whitespace-normalized match
        for cleaned_name, data in self.schema.items():
            if data['original_name'].strip().upper() == original_name.strip().upper():
                return cleaned_name
        
        return None
    
    def _add_primary_keys(self, pk_dict: Dict[str, List[str]]):
        """Add primary key information to schema.
        
        Args:
            pk_dict: Dict mapping original table names to primary key columns
        """
        for original_t, pkcols in pk_dict.items():
            found_cleaned_name = self._find_cleaned_name_for_original(original_t)
            
            if found_cleaned_name:
                self.schema[found_cleaned_name]['primary_keys'] = pkcols
            else:
                logger.warning(f"PK table '{original_t}' not found in schema based on original names.")
    
    def _add_foreign_keys(self, fk_list: List[Dict]):
        """Add foreign key information to schema.
        
        Args:
            fk_list: List of foreign key information dictionaries
        """
        for fk in fk_list:
            # Handle both new and legacy column naming
            original_parent_tbl = fk.get('parent_table', fk.get('parent_tbl', ''))
            
            found_cleaned_parent_name = self._find_cleaned_name_for_original(original_parent_tbl)
            
            if found_cleaned_parent_name:
                # Normalize FK info to new format
                normalized_fk = {
                    'name': fk.get('name', ''),
                    'parent_table': fk.get('parent_table', fk.get('parent_tbl', '')),
                    'parent_columns': fk.get('parent_columns', fk.get('parent_cols', [])),
                    'referenced_table': fk.get('referenced_table', fk.get('ref_tbl', '')),
                    'referenced_columns': fk.get('referenced_columns', fk.get('ref_cols', []))
                }
                self.schema[found_cleaned_parent_name]['foreign_keys'].append(normalized_fk)
            else:
                logger.warning(f"FK parent table '{original_parent_tbl}' not found in schema based on original names.")


def build_schema_dict(table_data_list: List[Dict], 
                     pk_dict: Dict[str, List[str]], 
                     fk_list: List[Dict]) -> Dict[str, Dict]:
    """Build schema dictionary from raw database metadata (standalone function).
    
    Args:
        table_data_list: List of dicts from get_table_structures
        pk_dict: Dict {original_table_name: [primary_key_columns]}
        fk_list: List of foreign key info
    
    Returns:
        dict: Schema dictionary with cleaned table names as keys
    """
    builder = SchemaBuilder()
    return builder.build_schema_dict(table_data_list, pk_dict, fk_list)


class SchemaAnalyzer:
    """Analyzer class for extracting insights from schema dictionaries."""
    
    @staticmethod
    def get_table_count(schema: Dict[str, Dict]) -> int:
        """Get total number of tables in schema.
        
        Args:
            schema: Schema dictionary
            
        Returns:
            int: Number of tables
        """
        return len(schema)
    
    @staticmethod
    def get_column_count(schema: Dict[str, Dict]) -> int:
        """Get total number of columns across all tables.
        
        Args:
            schema: Schema dictionary
            
        Returns:
            int: Total number of columns
        """
        return sum(len(table_info['columns']) for table_info in schema.values())
    
    @staticmethod
    def get_tables_with_primary_keys(schema: Dict[str, Dict]) -> List[str]:
        """Get list of table names that have primary keys.
        
        Args:
            schema: Schema dictionary
            
        Returns:
            List[str]: Table names with primary keys
        """
        return [table_name for table_name, table_info in schema.items() 
                if table_info['primary_keys']]
    
    @staticmethod
    def get_tables_with_foreign_keys(schema: Dict[str, Dict]) -> List[str]:
        """Get list of table names that have foreign keys.
        
        Args:
            schema: Schema dictionary
            
        Returns:
            List[str]: Table names with foreign keys
        """
        return [table_name for table_name, table_info in schema.items() 
                if table_info['foreign_keys']]
    
    @staticmethod
    def get_foreign_key_relationships(schema: Dict[str, Dict]) -> List[Tuple[str, str]]:
        """Get list of foreign key relationships.
        
        Args:
            schema: Schema dictionary
            
        Returns:
            List[Tuple[str, str]]: List of (parent_table, referenced_table) pairs
        """
        relationships = []
        for table_name, table_info in schema.items():
            for fk in table_info['foreign_keys']:
                parent_table = fk.get('parent_table', fk.get('parent_tbl', ''))
                referenced_table = fk.get('referenced_table', fk.get('ref_tbl', ''))
                relationships.append((parent_table, referenced_table))
        return relationships
