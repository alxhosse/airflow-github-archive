"""Generate statistics from Parquet files."""
import json
import logging
from pathlib import Path
from typing import Union

import pandas as pd

logger = logging.getLogger(__name__)


def generate_stats(
    parquet_path: Union[str, Path],
    output_path: Union[str, Path],
    overwrite: bool = False,
) -> str:
    """
    Generate statistics from a Parquet file and write to JSON.
    
    Args:
        parquet_path: Path to the input Parquet file (string or Path)
        output_path: Path where the JSON stats file should be saved (string or Path)
        overwrite: If True, overwrite existing file. If False, skip if file exists.
    
    Returns:
        String path to the output JSON file
    """
    # Convert strings to Path if needed
    parquet_path = Path(parquet_path) if isinstance(parquet_path, str) else parquet_path
    output_path = Path(output_path) if isinstance(output_path, str) else output_path
    
    # Create parent directory if it doesn't exist
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Skip if output already exists and not overwriting
    if output_path.exists() and not overwrite:
        logger.info(f"Stats file already exists: {output_path}")
        return str(output_path)
    
    if not parquet_path.exists():
        logger.warning(f"Parquet file not found: {parquet_path}")
        stats = {
            "total_events": 0,
            "error": "File not found",
        }
    else:
        try:
            # Read Parquet file
            df = pd.read_parquet(parquet_path, engine="pyarrow")
            
            if df.empty:
                logger.warning(f"Empty Parquet file: {parquet_path}")
                stats = {
                    "total_events": 0,
                }
            else:
                # Generate statistics
                stats = {
                    "total_events": len(df),
                    "event_types": df["type"].value_counts().to_dict() if "type" in df.columns else {},
                    "unique_actors": df["actor_login"].nunique() if "actor_login" in df.columns else 0,
                    "unique_repos": df["repo_name"].nunique() if "repo_name" in df.columns else 0,
                }
                
                # Add event type breakdown
                if "type" in df.columns:
                    stats["top_event_types"] = df["type"].value_counts().head(10).to_dict()
                
                # Add top repositories by event count
                if "repo_name" in df.columns:
                    stats["top_repos"] = df["repo_name"].value_counts().head(10).to_dict()
                
                # Add top actors by event count
                if "actor_login" in df.columns:
                    stats["top_actors"] = df["actor_login"].value_counts().head(10).to_dict()
                
                logger.info(f"Generated stats: {stats['total_events']} events")
        
        except Exception as e:
            logger.error(f"Failed to generate stats for {parquet_path}: {e}")
            stats = {
                "total_events": 0,
                "error": str(e),
            }
    
    # Write stats to JSON file
    logger.info(f"Writing stats to {output_path}")
    with open(output_path, "w") as f:
        json.dump(stats, f, indent=2)
    
    logger.info(f"Stats written OK: {output_path}")
    return str(output_path)

