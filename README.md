# Database Schema Grading System

A Python package for automated grading of database assignments with support for table/column matching, foreign key analysis, row count validation, view matching, and summary scoring. Now supports Gemini API for semantic matching.

## Features

- **Table Matching**: Intelligent table name matching using embeddings and fuzzy logic
- **Column Matching**: Advanced column matching with semantic similarity analysis
- **Foreign Key Analysis**: Compare foreign key relationships between schemas
- **Row Count Validation**: Compare data row counts between answer and student databases
- **View Matching**: Match views by column/row count and export results
- **Summary Table**: Aggregate grading results and compute detailed scores (A1, A2, A3, B, C, Tổng điểm)
- **Gemini API Integration**: Enhanced semantic understanding for better matching accuracy
- **.env Support**: API keys and secrets loaded automatically from `.env`

## Installation

1. **Install dependencies:**
```bash
pip install -r requirements.txt
```

2. **Setup configuration:**
```bash
# Create .env file in project root
# Example:
echo GOOGLE_API_KEY=your_gemini_api_key > .env
```

3. **Get Gemini API Key (optional but recommended):**
   - Visit: https://aistudio.google.com/app/apikey
   - Create a new API key
   - Add it to your `.env` file as `GOOGLE_API_KEY=your_key_here`

4. **Test installation:**
```bash
python setup.py test
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
│   │   ├── grading/            # Grading algorithms, summary, view matching
│   │   ├── foreign_key/        # Foreign key analysis
│   │   ├── embedding/          # AI/ML embeddings (Gemini, etc.)
│   │   └── utils/              # Utility functions
│   └── __init__.py
├── tests/                      # Test files
├── .env                        # API keys and secrets
├── requirements.txt
└── ...
```

## Version History

- **v1.4.3**: View matching, summary table with detailed points, Gemini embedding refactor, .env auto-load
- **v1.4**: View matching, summary table, robust pipeline
- **v1.3.x**: Row count, business logic, and foreign key improvements
- **v1.2**: Gemini API integration
- **v1.0**: Initial stable release

## Git Usage

- **Tag a release:**
  ```bash
  git tag v1.4.3
  git push --tags
  ```
- **Checkout a release:**
  ```bash
  git checkout v1.4.3
  ```

## License

MIT License
