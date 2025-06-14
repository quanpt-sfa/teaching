# Database Schema Grading System

A Python package for automated grading of database schemas with support for table/column matching, foreign key analysis, and row count validation.

## Features

- **Table Matching**: Intelligent table name matching using embeddings and fuzzy logic
- **Column Matching**: Advanced column matching with semantic similarity analysis
- **Foreign Key Analysis**: Compare foreign key relationships between schemas
- **Row Count Validation**: Compare data row counts between answer and student databases
- **Gemini API Integration**: Enhanced semantic understanding for better matching accuracy

## Installation

```bash
pip install -r requirements.txt
```

## Usage

### Basic Usage

```python
from v1.cli.grade_bak import grade_single_database, grade_batch

# Grade a single database
result = grade_single_database('student.bak', answer_schema, config)

# Grade multiple databases
results = grade_batch('backup_folder/', answer_schema, config)
```

### Configuration

```python
from v1.schema_grader.config import GradingConfig

config = GradingConfig(
    server='localhost',
    user='sa', 
    password='password',
    data_folder='C:/temp/',
    output_folder='results/'
)
```

## Project Structure

```
grading/
├── v1/
│   ├── cli/                    # Command line interfaces
│   ├── schema_grader/          # Core grading package
│   │   ├── db/                 # Database operations
│   │   ├── matching/           # Table/column matching logic
│   │   ├── grading/            # Grading algorithms
│   │   ├── foreign_key/        # Foreign key analysis
│   │   ├── embedding/          # AI/ML embeddings
│   │   └── utils/              # Utility functions
│   └── __init__.py
├── tests/                      # Test files
├── temp/                       # Temporary/cache files
└── requirements.txt
```

## Version History

- **v1.2**: Enhanced matching with Gemini API integration
- **v1.1**: Foreign key matching improvements  
- **v1.0**: Initial stable release

## License

MIT License
