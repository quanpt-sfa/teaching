import re
from ..utils.constants import STAGE_RE
from .apply_alias import apply_alias

def clean_rows(raw_table_data_list):
    """Làm sạch dữ liệu các hàng từ DB.
    
    - Loại bỏ bảng stage
    - Xóa tiền tố số (đã được xử lý trong schema_reader._clean_table_name)
    - Áp dụng alias cho tên bảng (đã được xử lý trong schema_reader._clean_table_name nếu alias là một phần của chuẩn hóa)
    - Áp dụng alias cho tên cột
    
    Args:
        raw_table_data_list: List of dicts from get_table_structures, 
                             each {'original_name': str, 'cleaned_name': str, 'column_name': str, 'data_type': str}
        
    Returns:
        list: List of dicts with potentially modified column names and filtered tables.
              The structure of each dict remains the same.
    """
    cleaned_data_list = []
    for item in raw_table_data_list:
        original_t = item['original_name']
        cleaned_t = item['cleaned_name'] # This is already cleaned (e.g. prefix removed, uppercased)
        c = item['column_name']
        d = item['data_type']        # Filtering stage tables based on original name
        if STAGE_RE.search(original_t) or original_t.strip().lower().startswith('stage'):
            continue
        
        # The table name cleaning (prefix, to_upper) is now done in schema_reader._clean_table_name.
        # If apply_alias for table names is part of that cleaning, it's already handled.
        # Here, we primarily focus on applying alias to column names.
        # The cleaned_t from schema_reader is what we should use going forward for the table identifier.

        # Create a new dictionary for the cleaned item to avoid modifying the original list items directly
        # if raw_table_data_list is used elsewhere.
        cleaned_item = {
            'original_name': original_t,
            'cleaned_name': cleaned_t, # Use the already cleaned table name
            'column_name': apply_alias(c), # Apply alias only to column name here
            'data_type': d
        }
        cleaned_data_list.append(cleaned_item)
        
    return cleaned_data_list
