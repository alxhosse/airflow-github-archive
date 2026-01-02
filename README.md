# GH Archive Airflow Project

A Docker-based Apache Airflow project for downloading and processing GitHub Archive data.

## Overview

This project provides a complete pipeline to:
- Download hourly GitHub Archive data from [gharchive.org](https://www.gharchive.org/)
- Transform JSON.gz files to Parquet format with extracted important columns
- Generate statistics from the processed data

## Project Structure

```
gh-archive-airflow/
├── dags/
│   └── gh_archive_hourly_dag.py    # DAG definition
├── src/
│   └── gh_archive/
│       ├── __init__.py
│       ├── jobs/
│       │   ├── __init__.py
│       │   ├── fetch.py             # Download GH Archive files (context-free)
│       │   ├── transform.py         # JSON.gz -> Parquet transformation (context-free)
│       │   └── stats.py             # Generate statistics from Parquet (context-free)
│       └── utils/
│           ├── __init__.py
│           ├── paths.py             # Partitioned path utilities
│           └── io.py                # Atomic write helpers
├── tests/                            # Test suite
│   ├── test_fetch.py                # Tests for fetch module
│   ├── test_transform.py            # Tests for transform module
│   └── test_stats.py                # Tests for stats module
├── data/                             # Local data storage (partitioned)
│   ├── raw/                          # Raw JSON.gz files
│   ├── clean/                        # Processed Parquet files
│   └── stats/                        # Statistics JSON files
├── compose.yml                       # Docker Compose configuration
├── compose.override.yaml             # Local overrides
├── pyproject.toml                    # Project configuration (uv/pip)
├── pytest.ini                       # Pytest configuration
├── requirements.txt                  # Python dependencies (legacy)
└── README.md                         # This file
```

## Prerequisites

- **Docker Desktop** (or Docker Engine + Docker Compose)
- **4GB+ RAM** available for Docker
- **2+ CPUs** recommended
- **10GB+ disk space** available (more for historical data)
- **uv** (recommended) or **pip** for Python package management

## Development Setup

### Using uv (Recommended)

[uv](https://github.com/astral-sh/uv) is a fast Python package installer and resolver.

```bash
# Install uv (if not already installed)
curl -LsSf https://astral.sh/uv/install.sh | sh

# Create virtual environment and install dependencies
uv venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install project and dev dependencies
uv pip install -e ".[dev]"

# Run tests
pytest
```

### Using pip

```bash
# Create virtual environment
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

## Quick Start

### 1. Set Up Environment Variables

Create a `.env` file:

**On Linux/Mac:**
```bash
echo "AIRFLOW_UID=$(id -u)" > .env
```

**On Windows:**
Edit `.env` and set `AIRFLOW_UID=50000`

### 2. Start Airflow

```bash
docker compose up -d
```

This will start all Airflow services:
- **PostgreSQL** - Metadata database
- **Redis** - Message broker for Celery
- **Airflow Webserver** - UI at http://localhost:8081
- **Airflow Scheduler** - Schedules and triggers DAGs
- **Airflow Worker** - Executes tasks

### 3. Access Airflow UI

Wait a few seconds for initialization, then open:
- **Web UI**: http://localhost:8081
- **Default credentials**: 
  - Username: `airflow`
  - Password: `airflow`

### 4. Configure Data Directories (Optional)

By default, data is stored in `./data` with subdirectories for raw, clean, and stats. You can customize these via Airflow Variables:

1. Go to Admin → Variables in the Airflow UI
2. Add variables (all optional):
   - `GH_ARCHIVE_RAW_DIR` - Raw JSON.gz files (default: `./data/raw`)
   - `GH_ARCHIVE_CLEAN_DIR` - Processed Parquet files (default: `./data/clean`)
   - `GH_ARCHIVE_STATS_DIR` - Statistics JSON files (default: `./data/stats`)

## DAG Description

The `gh_archive_hourly` DAG runs hourly using an interval-based timetable (`CronDataIntervalTimetable`) and performs three tasks:

1. **fetch_hourly_archive**: Downloads the hourly JSON.gz file from GH Archive
2. **transform_to_parquet**: Transforms JSON lines to Parquet format, extracting important columns
3. **generate_stats**: Generates statistics from the Parquet file and writes to JSON

### Architecture

- **Interval-based scheduling**: Uses `CronDataIntervalTimetable` for proper data interval handling
- **Context-free jobs**: All job functions are Airflow-agnostic and can be used independently
- **No XCom dependencies**: Each task builds its own paths using the `build_paths()` function
- **Partitioned storage**: Data is organized by year/month/day/hour for efficient querying

### Data Partitioning

Data is stored in a partitioned structure across three directories:

```
data/
├── raw/                              # Raw JSON.gz files
│   └── year=YYYY/
│       └── month=MM/
│           └── day=DD/
│               └── hour=HH/
│                   └── events.json.gz
├── clean/                            # Processed Parquet files
│   └── year=YYYY/
│       └── month=MM/
│           └── day=DD/
│               └── hour=HH/
│                   └── events.parquet
└── stats/                            # Statistics JSON files
    └── year=YYYY/
        └── month=MM/
            └── day=DD/
                └── hour=HH/
                    └── stats.json
```

### Extracted Columns

The transformation extracts the following important columns from GitHub events:
- Event metadata: `id`, `type`, `created_at`, `public`
- Actor information: `actor_id`, `actor_login`, `actor_type`
- Repository information: `repo_id`, `repo_name`, `repo_url`
- Organization information: `org_id`, `org_login`
- Payload information: `payload_action`, `payload_size`, `payload_distinct_size`

## Common Commands

### Start Services
```bash
docker compose up -d
```

### Stop Services
```bash
docker compose down
```

### View Logs
```bash
# All services
docker compose logs -f

# Specific service
docker compose logs -f airflow-scheduler
docker compose logs -f airflow-worker
```

### Access Airflow CLI
```bash
docker compose run airflow-cli airflow dags list
docker compose run airflow-cli airflow tasks list gh_archive_hourly
```

## Data Sources

This project downloads data from [GH Archive](https://www.gharchive.org/), which provides:
- Hourly archives of GitHub public timeline events
- Data available from February 12, 2011 onwards
- 15+ event types (commits, forks, issues, pull requests, etc.)

Example URL format:
```
https://data.gharchive.org/2024-01-01-15.json.gz
```

## Development

### Adding Dependencies

Add packages to `requirements.txt`, then rebuild the Docker image or use `_PIP_ADDITIONAL_REQUIREMENTS` environment variable (not recommended for production).

### Testing

The project includes comprehensive test coverage for all job modules. See [tests/README.md](tests/README.md) for details.

**Run tests:**
```bash
# Using uv (recommended)
uv pip install -e ".[dev]"
pytest

# Using pip
pip install -r requirements.txt
pytest
```

**Test coverage:**
- `test_fetch.py` - 14 tests for download functionality
- `test_transform.py` - 15 tests for transformation and column extraction
- `test_stats.py` - 16 tests for statistics generation

### Testing Locally

You can test the modules directly (all functions are context-free):

```python
from gh_archive.jobs.fetch import download_file
from gh_archive.jobs.transform import transform_json_to_parquet
from gh_archive.jobs.stats import generate_stats

# Download a file
raw_path = download_file(
    url="https://data.gharchive.org/2024-01-01-15.json.gz",
    output_path="./data/raw/year=2024/month=01/day=01/hour=15/events.json.gz"
)

# Transform to Parquet
clean_path = transform_json_to_parquet(
    input_gz_path=raw_path,
    output_parquet_path="./data/clean/year=2024/month=01/day=01/hour=15/events.parquet"
)

# Generate statistics
stats_path = generate_stats(
    parquet_path=clean_path,
    output_path="./data/stats/year=2024/month=01/day=01/hour=15/stats.json"
)
```

## Troubleshooting

### DAG Not Appearing
- Check that `src/` directory is mounted correctly
- Verify PYTHONPATH includes `/opt/airflow/src`
- Check logs: `docker compose logs airflow-scheduler`

### Download Failures
- GH Archive files are typically available ~1 hour after the hour completes
- Check network connectivity
- Verify the date/hour is valid (data starts from 2011-02-12)
- For manual triggers, ensure you're using a past date (not future)
- The DAG uses `data_interval_start` directly - no need to subtract hours with interval-based timetable

### Permission Issues
- Ensure `AIRFLOW_UID` is set correctly in `.env`
- Check file permissions on `data/` directory

## Resources

- [GH Archive Documentation](https://www.gharchive.org/)
- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [Airflow Docker Documentation](https://airflow.apache.org/docs/docker-stack/index.html)

## License

This project uses Apache Airflow, which is licensed under the Apache License 2.0.

