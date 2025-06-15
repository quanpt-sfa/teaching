"""
Database schema reading utilities for grading system.
"""

import re
from typing import Dict, List, Tuple, Any
from ..utils.logger import get_logger

logger = get_logger(__name__)


def clean_table_name(name: str) -> str:
    """Clean table name by removing numeric prefixes and normalizing case.
    
    Args:
        name: Original table name
        
    Returns:
        str: Cleaned table name in uppercase
    """
    # Remove numeric prefixes like "08." or "07. " (with optional space after dot)
    cleaned = re.sub(r'^\d+\.\s*', '', name)
    # Convert to uppercase for consistency
    return cleaned.upper()


def get_table_structures(connection) -> List[Dict[str, str]]:
    """Get table structure information from database.
    
    Args:
        connection: Database connection object
        
    Returns:
        List[Dict]: List of table structure data with original_name, cleaned_name, column_name, data_type
    """
    sql = """
        SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE LEFT(TABLE_NAME, 3) <> 'sys'
          AND TABLE_NAME IN (
            SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_TYPE='BASE TABLE' AND LEFT(TABLE_NAME,3) <> 'sys')
        ORDER BY TABLE_NAME, ORDINAL_POSITION"""
    
    try:
        cursor = connection.cursor()
        rows = cursor.execute(sql).fetchall()
        
        table_data = []
        raw_table_names = set()
        
        for original_t, c, d in rows:
            raw_table_names.add(original_t)
            cleaned_t = clean_table_name(original_t)
            table_data.append({
                'original_name': original_t, 
                'cleaned_name': cleaned_t, 
                'column_name': c, 
                'data_type': d
            })
        
        logger.debug(f"Raw table names from DB: {sorted(raw_table_names)}")
        logger.debug(f"Sample cleaning - '08.CT_ChiTien' -> '{clean_table_name('08.CT_ChiTien')}'")
        logger.debug(f"Sample cleaning - '07.ChiTien' -> '{clean_table_name('07.ChiTien')}'")
        logger.debug(f"Sample cleaning - '07. CHITIEN' -> '{clean_table_name('07. CHITIEN')}'")
        
        return table_data
        
    except Exception as e:
        logger.error(f"Error reading table structures: {e}")
        raise


def get_primary_keys(connection) -> Dict[str, List[str]]:
    """Get primary key information from database.
    
    Args:
        connection: Database connection object
        
    Returns:
        Dict[str, List[str]]: Mapping of table names to primary key columns
    """
    sql = """
        SELECT KU.TABLE_NAME, KU.COLUMN_NAME
        FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS TC
        JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS KU
            ON TC.CONSTRAINT_TYPE='PRIMARY KEY' AND TC.CONSTRAINT_NAME = KU.CONSTRAINT_NAME
        WHERE LEFT(KU.TABLE_NAME,3) <> 'sys'
        ORDER BY KU.TABLE_NAME, KU.ORDINAL_POSITION"""
    
    try:
        cursor = connection.cursor()
        pk = {}
        for tbl_original_name, col in cursor.execute(sql):
            pk.setdefault(tbl_original_name, []).append(col)
        
        logger.debug(f"Found primary keys for {len(pk)} tables")
        return pk
        
    except Exception as e:
        logger.error(f"Error reading primary keys: {e}")
        raise


def get_foreign_keys(connection) -> List[Dict[str, Any]]:
    """Get foreign key information from database.
    
    Args:
        connection: Database connection object
        
    Returns:
        List[Dict]: List of foreign key information
    """
    sql = """
        SELECT fk.name, tp.name, cp.name, ref.name, cr.name
        FROM sys.foreign_keys fk
        JOIN sys.tables tp    ON fk.parent_object_id     = tp.object_id
        JOIN sys.tables ref   ON fk.referenced_object_id = ref.object_id
        JOIN sys.foreign_key_columns fkc
             ON fk.object_id = fkc.constraint_object_id
        JOIN sys.columns cp   ON fkc.parent_object_id = cp.object_id
                             AND fkc.parent_column_id = cp.column_id
        JOIN sys.columns cr   ON fkc.referenced_object_id = cr.object_id
                             AND fkc.referenced_column_id = cr.column_id
        WHERE LEFT(tp.name,3)<>'sys' AND LEFT(ref.name,3)<>'sys'
        ORDER BY fk.name"""
    
    try:
        cursor = connection.cursor()
        rows = cursor.execute(sql).fetchall()
        
        fk_dict = {}
        for fk_name, p_tbl_original, fk_col, r_tbl_original, pk_col in rows:
            key = (fk_name, p_tbl_original, r_tbl_original)

            fk_dict.setdefault(key, {
                'name': fk_name,
                'parent_table': p_tbl_original,  # Store original name
                'parent_columns': [], 
                'referenced_table': r_tbl_original,   # Store original name
                'referenced_columns': []
            })
            fk_dict[key]['parent_columns'].append(fk_col)
            fk_dict[key]['referenced_columns'].append(pk_col)
        
        result = list(fk_dict.values())
        logger.debug(f"Found {len(result)} foreign key constraints")
        return result
        
    except Exception as e:
        logger.error(f"Error reading foreign keys: {e}")
        raise


# Legacy function alias for backward compatibility
def get_foreign_keys_full(connection):
    """Legacy alias for get_foreign_keys with old column names."""
    fks = get_foreign_keys(connection)
    # Convert to old format for backward compatibility
    legacy_fks = []
    for fk in fks:
        legacy_fks.append({
            'parent_tbl': fk['parent_table'],
            'parent_cols': fk['parent_columns'],
            'ref_tbl': fk['referenced_table'],
            'ref_cols': fk['referenced_columns']
        })
    return legacy_fks


class SchemaReader:
    """Database schema reading utilities."""
    
    def __init__(self):
        """Initialize schema reader."""
        pass
    
    def clean_table_name(self, name: str) -> str:
        """Clean table name by removing numeric prefixes and normalizing case."""
        return clean_table_name(name)
    
    def clean_column_name(self, name: str) -> str:
        """Clean column name by normalizing case and format."""
        return clean_column_name(name)
    
    def read_schema(self, connection, use_cleaned_names: bool = True) -> Dict[str, Dict]:
        """Read database schema and return structured data."""
        return read_db_schema(connection, use_cleaned_names)


# Legacy functions remain for backward compatibility
