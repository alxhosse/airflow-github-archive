"""GH Archive hourly data pipeline DAG.

This DAG downloads hourly GitHub Archive data, transforms it to Parquet,
and generates statistics.
"""
import logging
from datetime import datetime, timedelta
from typing import NamedTuple

import pendulum
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, Variable
from airflow.timetables.interval import CronDataIntervalTimetable


from gh_archive.jobs.fetch import download_file
from gh_archive.jobs.stats import generate_stats
from gh_archive.jobs.transform import transform_json_to_parquet
from gh_archive.utils.paths import get_gh_archive_url

logger = logging.getLogger(__name__)


class Paths(NamedTuple):
    """Container for all paths used in the pipeline."""
    raw_path: str
    clean_path: str
    stats_path: str
    url: str


def _get_dir(variable_name: str, default: str) -> str:
    """Get directory from Airflow Variable or use default."""
    try:
        return Variable.get(variable_name)
    except Exception:
        return default


def build_paths(dt: datetime, raw_dir: str, clean_dir: str, stats_dir: str) -> Paths:
    """
    Build all paths for a given datetime.
    
    Args:
        dt: Datetime object (should be data_interval_start)
        raw_dir: Base directory for raw data
        clean_dir: Base directory for clean/parquet data
        stats_dir: Base directory for stats JSON files
    
    Returns:
        Paths named tuple with all required paths
    """
    year, month, day, hour = dt.year, dt.month, dt.day, dt.hour
    
    # Build partitioned paths
    raw_path = f"{raw_dir}/year={year:04d}/month={month:02d}/day={day:02d}/hour={hour:02d}/events.json.gz"
    clean_path = f"{clean_dir}/year={year:04d}/month={month:02d}/day={day:02d}/hour={hour:02d}/events.parquet"
    stats_path = f"{stats_dir}/year={year:04d}/month={month:02d}/day={day:02d}/hour={hour:02d}/stats.json"
    
    # Get URL
    url = get_gh_archive_url(year, month, day, hour)
    
    return Paths(raw_path=raw_path, clean_path=clean_path, stats_path=stats_path, url=url)


with DAG(
    dag_id="gh_archive_hourly",
    description="Download and process hourly GH Archive data",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule=CronDataIntervalTimetable("10 * * * *", timezone="UTC"),  # Every hour at minute 10
    catchup=False,  # Don't backfill automatically
    default_args={
        "owner": "data-engineering",
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["gh-archive", "github", "data-pipeline"],
    max_active_runs=1,  # Only one run at a time
) as dag:
    
    # Retrieve directory variables once at DAG level
    RAW_DIR = _get_dir("GH_ARCHIVE_RAW_DIR", "./data/raw")
    CLEAN_DIR = _get_dir("GH_ARCHIVE_CLEAN_DIR", "./data/clean")
    STATS_DIR = _get_dir("GH_ARCHIVE_STATS_DIR", "./data/stats")
    
    def fetch_task(**context):
        """Fetch hourly archive data."""
        # Use data_interval_start directly (no need to subtract 1 hour with interval-based timetable)
        data_interval_start = context["data_interval_start"]
        paths = build_paths(data_interval_start, RAW_DIR, CLEAN_DIR, STATS_DIR)
        
        logger.info(f"Fetching data for {data_interval_start}: {paths.url} -> {paths.raw_path}")
        return download_file(paths.url, paths.raw_path, overwrite=False)
    
    def transform_task(**context):
        """Transform JSON.gz to Parquet."""
        data_interval_start = context["data_interval_start"]
        paths = build_paths(data_interval_start, RAW_DIR, CLEAN_DIR, STATS_DIR)
        
        logger.info(f"Transforming data for {data_interval_start}: {paths.raw_path} -> {paths.clean_path}")
        return transform_json_to_parquet(paths.raw_path, paths.clean_path, overwrite=False)
    
    def stats_task(**context):
        """Generate statistics from Parquet."""
        data_interval_start = context["data_interval_start"]
        paths = build_paths(data_interval_start, RAW_DIR, CLEAN_DIR, STATS_DIR)
        
        logger.info(f"Generating stats for {data_interval_start}: {paths.clean_path} -> {paths.stats_path}")
        return generate_stats(paths.clean_path, paths.stats_path, overwrite=False)
    
    # Define tasks
    fetch = PythonOperator(
        task_id="fetch_hourly_archive",
        python_callable=fetch_task,
    )
    
    transform = PythonOperator(
        task_id="transform_to_parquet",
        python_callable=transform_task,
    )
    
    stats = PythonOperator(
        task_id="generate_stats",
        python_callable=stats_task,
    )
    
    # Define task dependencies
    fetch >> transform >> stats

