from collections import defaultdict

def build_schema_dict(table_data_list, pk_dict, fk_list):
    """Xây dựng cấu trúc schema từ dữ liệu thô.
    
    Args:
        table_data_list: List of dicts from get_table_structures, each {'original_name': str, 'cleaned_name': str, 'column_name': str, 'data_type': str}
        pk_dict: Dict {original_table_name: [primary_key_columns]}
        fk_list: List các foreign key info, where table names are original
    
    Returns:
        dict: Schema với cấu trúc {cleaned_table_name: {'original_name': str, 'cols': [], 'pk': [], 'fks': []}}
    """
    schema = defaultdict(lambda: {'original_name': '', 'cols': [], 'pk': [], 'fks': []})
    
    # Debug: Print all table data we're processing
    print(f"Debug: Processing {len(table_data_list)} table data items...")
    original_names_seen = set()
    
    # Populate schema with columns and original names, keyed by cleaned names
    for item in table_data_list:
        cleaned_t = item['cleaned_name']
        original_t = item['original_name']
        
        original_names_seen.add(original_t)
        
        if not schema[cleaned_t]['original_name']: # Store original name, first one encountered for a cleaned name
            schema[cleaned_t]['original_name'] = original_t
        elif schema[cleaned_t]['original_name'] != original_t:
            # This case (multiple original names map to the same cleaned name) needs careful handling.
            # For now, we'll overwrite, but ideally, this should be resolved or logged.
            # Or, the keying of the schema itself might need to be original_name if cleaned_name is not unique enough.
            # For simplicity, let's assume cleaned_name is the primary key for the schema dict for matching purposes.
            # The first original_name encountered will be stored.
            print(f"Warning: Cleaned name '{cleaned_t}' maps to multiple original names: '{schema[cleaned_t]['original_name']}' and '{original_t}'. Using the first one.")

        schema[cleaned_t]['cols'].append((item['column_name'], item['data_type']))

    print(f"Debug: Original table names seen in data: {sorted(original_names_seen)}")
    print(f"Debug: PK dict keys: {sorted(pk_dict.keys())}")
    print(f"Debug: FK parent tables: {sorted(set(fk['parent_tbl'] for fk in fk_list))}")# Add PKs, assuming pk_dict is keyed by original table names
    for original_t, pkcols in pk_dict.items():
        # Find the corresponding cleaned_name for this original_t
        # Try exact match first, then try case-insensitive match
        found_cleaned_name = None
        for cn, data in schema.items():
            if data['original_name'] == original_t:
                found_cleaned_name = cn
                break
        
        # If exact match failed, try case-insensitive and whitespace-normalized match
        if not found_cleaned_name:
            for cn, data in schema.items():
                if data['original_name'].strip().upper() == original_t.strip().upper():
                    found_cleaned_name = cn
                    break
        
        if found_cleaned_name:
            schema[found_cleaned_name]['pk'] = pkcols
        else:
            # This original_t from pk_dict was not in table_data_list, or its original_name wasn't stored
            print(f"Warning: PK table '{original_t}' not found in schema based on original names.")    # Add FKs, assuming fk_list uses original table names
    for fk in fk_list:
        original_parent_tbl = fk['parent_tbl']
        # Find the cleaned name for the parent table
        found_cleaned_parent_name = None
        for cn, data in schema.items():
            if data['original_name'] == original_parent_tbl:
                found_cleaned_parent_name = cn
                break
        
        # If exact match failed, try case-insensitive and whitespace-normalized match
        if not found_cleaned_parent_name:
            for cn, data in schema.items():
                if data['original_name'].strip().upper() == original_parent_tbl.strip().upper():
                    found_cleaned_parent_name = cn
                    break
        
        if found_cleaned_parent_name:
            # Store the FK. If FKs also need to reference cleaned names internally for matching,
            # that conversion would happen here or when the FKs are initially processed.
            # For now, store as is, assuming subsequent logic can handle original names if needed.
            schema[found_cleaned_parent_name]['fks'].append(fk)
        else:
            print(f"Warning: FK parent table '{original_parent_tbl}' not found in schema based on original names.")
            
    return dict(schema)
