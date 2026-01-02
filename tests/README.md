# Tests

This directory contains comprehensive tests for the GH Archive Airflow project.

## Running Tests

### Using uv (Recommended)

```bash
# Install dependencies (including dev dependencies)
uv pip install -e ".[dev]"

# Run all tests
pytest

# Run with verbose output
pytest -v

# Run specific test file
pytest tests/test_fetch.py -v
pytest tests/test_transform.py -v
pytest tests/test_stats.py -v
```

### Using pip

```bash
# Install dependencies
pip install -r requirements.txt

# Run all tests
pytest
```

### Run with Coverage

```bash
pytest --cov=src/gh_archive/jobs --cov-report=html
```

This will generate an HTML coverage report in `htmlcov/index.html`.

### Run Specific Test Classes or Methods

```bash
# Run specific test class
pytest tests/test_fetch.py::TestDownloadFile -v

# Run specific test method
pytest tests/test_transform.py::TestExtractImportantColumns::test_extract_actor_info -v
```

## Test Structure

- `test_fetch.py` - Tests for `download_file()` function in `gh_archive.jobs.fetch`
- `test_transform.py` - Tests for `extract_important_columns()` and `transform_json_to_parquet()` in `gh_archive.jobs.transform`
- `test_stats.py` - Tests for `generate_stats()` function in `gh_archive.jobs.stats`

## Test Coverage

### test_fetch.py (14 tests)
- ✅ Successful downloads
- ✅ File existence checks (with and without overwrite)
- ✅ Directory creation
- ✅ Error handling (HTTP errors, connection errors, timeouts)
- ✅ Atomic file writes using temp files
- ✅ Large file handling (chunked downloads)
- ✅ Empty file downloads
- ✅ Empty chunk skipping
- ✅ Temp file cleanup on errors
- ✅ Path object and string handling
- ✅ Return type validation

### test_transform.py (15 tests)
- ✅ Column extraction (basic fields, actor, repo, org, payload)
- ✅ Missing field handling
- ✅ Successful JSON.gz to Parquet transformation
- ✅ File existence checks (with and without overwrite)
- ✅ Directory creation
- ✅ Empty file handling
- ✅ Invalid JSON line handling
- ✅ Large file chunked processing (15k+ events)
- ✅ Path object and string handling
- ✅ Atomic writes using temp files
- ✅ Temp file cleanup on errors
- ✅ Return type validation

### test_stats.py (16 tests)
- ✅ Successful statistics generation
- ✅ File existence checks (with and without overwrite)
- ✅ Directory creation
- ✅ Missing Parquet file handling
- ✅ Empty Parquet file handling
- ✅ Top event types generation
- ✅ Top repositories generation
- ✅ Top actors generation
- ✅ Missing column handling
- ✅ Error handling during processing
- ✅ Path object and string handling
- ✅ JSON format validation
- ✅ Return type validation

## Test Statistics

- **Total tests**: 49
- **Test files**: 3
- **Coverage**: All job functions (`fetch`, `transform`, `stats`)
- **Test style**: Unit tests using pytest with mocking

## Writing New Tests

When adding new functionality, follow these patterns:

1. **Use pytest fixtures**: `tmp_path` for temporary files
2. **Mock external dependencies**: Use `unittest.mock` for network calls, file I/O
3. **Test edge cases**: Empty files, missing files, invalid data
4. **Test both Path and string inputs**: All functions accept both types
5. **Verify return types**: Functions return strings (for XCom compatibility)
6. **Test error handling**: Ensure proper cleanup on failures

Example test structure:
```python
def test_my_function(self, tmp_path):
    """Test description."""
    # Arrange
    input_file = tmp_path / "input.txt"
    output_file = tmp_path / "output.txt"
    
    # Act
    result = my_function(input_file, output_file)
    
    # Assert
    assert result == str(output_file)
    assert output_file.exists()
```

