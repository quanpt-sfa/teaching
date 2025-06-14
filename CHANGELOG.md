# Changelog

All notable changes to the Database Schema Grading System will be documented in this file.

## [1.3.2] - 2025-06-14

### Fixed
- **Critical**: Fixed STAGE_RE regex that was incorrectly filtering out student tables with numeric prefixes (e.g., `01.HangTonKho`, `07.ChiTien`)
- **Row Count Logic**: Enhanced table name handling to preserve original student table names for database queries
- **Schema Building**: Improved original name preservation throughout the schema building pipeline
- **Mapping Enhancement**: Updated table matching to store both cleaned and original names for accurate querying

### Changed
- Modified `STAGE_RE` pattern from `r'^\d+\.'` to `r'stage'` with case-insensitive matching
- Updated `clean_data.py` to use `search()` instead of `match()` for stage table detection
- Enhanced `build_schema_dict()` with case-insensitive and whitespace-normalized name matching
- Added comprehensive debug logging for table name processing

### Added
- Debug output for raw table names from database
- Enhanced error handling for PK/FK table name mismatches
- Case-insensitive fallback matching for original table names

### Technical Details
- `schema_reader.py`: Enhanced `get_table_structures()` to return original and cleaned names
- `build_schema.py`: Improved original name association and fuzzy matching
- `table_matcher.py`: Updated to work with new schema structure containing original names
- `row_count_checker.py`: Modified to accept and use answer_schema for original name lookup
- `pipeline.py`: Updated to pass answer_schema to row count checking functions

## [1.3.1] - 2025-06-14

### Enhanced
- Comprehensive row count analysis for all mapped tables
- Improved business logic detection and scoring
- Better error handling for unmapped tables
- Enhanced CSV output formatting

### Fixed
- UnboundLocalError in row count formatting
- Indentation and syntax errors
- Missing table analysis in row count checking

## [1.3.0] - 2025-06-14

### Added
- Complete row count checking system
- Business logic table analysis
- Data import correctness validation
- Comprehensive CSV reporting with Vietnamese headers

### Features
- Analysis of all mapped tables (not just business logic tables)
- Distinction between data import and business logic correctness
- Robust error handling for missing or unmapped tables
- Excel-compatible CSV formatting
