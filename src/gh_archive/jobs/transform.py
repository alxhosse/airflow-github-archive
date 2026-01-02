"""Transform JSON.gz files to Parquet format."""
import gzip
import json
import logging
from pathlib import Path
from typing import Union

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

logger = logging.getLogger(__name__)

# Chunk size for processing events
CHUNK_SIZE = 10000


def extract_important_columns(event: dict) -> dict:
    """
    Extract important columns from a GitHub event.
    
    Args:
        event: Raw GitHub event dictionary
    
    Returns:
        Dictionary with extracted important fields
    """
    # Extract common fields
    extracted = {
        "id": event.get("id"),
        "type": event.get("type"),
        "created_at": event.get("created_at"),
        "public": event.get("public"),
    }
    
    # Extract actor information
    actor = event.get("actor", {})
    if isinstance(actor, dict):
        extracted["actor_id"] = actor.get("id")
        extracted["actor_login"] = actor.get("login")
        extracted["actor_type"] = actor.get("type")
    else:
        extracted["actor_id"] = None
        extracted["actor_login"] = None
        extracted["actor_type"] = None
    
    # Extract repository information
    repo = event.get("repo", {})
    if isinstance(repo, dict):
        extracted["repo_id"] = repo.get("id")
        extracted["repo_name"] = repo.get("name")
        extracted["repo_url"] = repo.get("url")
    else:
        extracted["repo_id"] = None
        extracted["repo_name"] = None
        extracted["repo_url"] = None
    
    # Extract organization information if available
    org = event.get("org", {})
    if isinstance(org, dict):
        extracted["org_id"] = org.get("id")
        extracted["org_login"] = org.get("login")
    else:
        extracted["org_id"] = None
        extracted["org_login"] = None
    
    # Extract payload action for certain event types
    payload = event.get("payload", {})
    if isinstance(payload, dict):
        extracted["payload_action"] = payload.get("action")
        # For PushEvent, extract commit count
        if event.get("type") == "PushEvent":
            extracted["payload_size"] = payload.get("size")
            extracted["payload_distinct_size"] = payload.get("distinct_size")
        else:
            extracted["payload_size"] = None
            extracted["payload_distinct_size"] = None
    else:
        extracted["payload_action"] = None
        extracted["payload_size"] = None
        extracted["payload_distinct_size"] = None
    
    return extracted


def transform_json_to_parquet(
    input_gz_path: Union[str, Path],
    output_parquet_path: Union[str, Path],
    overwrite: bool = False,
) -> str:
    """
    Transform a JSON.gz file to Parquet format.
    
    Args:
        input_gz_path: Path to the input JSON.gz file (string or Path)
        output_parquet_path: Path where the Parquet file should be saved (string or Path)
        overwrite: If True, overwrite existing file. If False, skip if file exists.
    
    Returns:
        String path to the output Parquet file
    """
    # Convert strings to Path if needed
    input_gz_path = Path(input_gz_path) if isinstance(input_gz_path, str) else input_gz_path
    output_parquet_path = Path(output_parquet_path) if isinstance(output_parquet_path, str) else output_parquet_path
    
    # Create parent directory if it doesn't exist
    output_parquet_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Skip if parquet already exists and not overwriting
    if output_parquet_path.exists() and not overwrite:
        logger.info(f"Parquet file already exists: {output_parquet_path}")
        return str(output_parquet_path)
    
    logger.info(f"Transforming {input_gz_path} -> {output_parquet_path}")
    
    # Use temporary file for atomic write
    temp_path = output_parquet_path.parent / f".{output_parquet_path.name}.tmp"
    
    try:
        # Process in chunks to avoid memory issues
        events_chunk = []
        total_events = 0
        parquet_writer = None
        schema = None
        
        with gzip.open(input_gz_path, "rt", encoding="utf-8") as f:
            for line_num, line in enumerate(f, 1):
                try:
                    event = json.loads(line.strip())
                    if event:  # Skip empty lines
                        extracted = extract_important_columns(event)
                        events_chunk.append(extracted)
                        total_events += 1
                        
                        # Process chunk when it reaches CHUNK_SIZE
                        if len(events_chunk) >= CHUNK_SIZE:
                            df_chunk = pd.DataFrame(events_chunk)
                            
                            # Initialize writer with schema from first chunk
                            if parquet_writer is None:
                                table = pa.Table.from_pandas(df_chunk)
                                schema = table.schema
                                parquet_writer = pq.ParquetWriter(
                                    temp_path,
                                    schema=schema,
                                    compression="snappy",
                                )
                            
                            # Write chunk - convert to match schema if needed
                            table = pa.Table.from_pandas(df_chunk)
                            # Cast to match original schema to ensure consistency
                            if schema is not None:
                                table = table.cast(schema)
                            parquet_writer.write_table(table)
                            events_chunk = []
                            
                            # Log progress every 100k events
                            if total_events % 100000 == 0:
                                logger.info(f"Processed {total_events} events so far...")
                                
                except json.JSONDecodeError as e:
                    logger.warning(f"Skipping invalid JSON on line {line_num}: {e}")
                    continue
                except Exception as e:
                    logger.warning(f"Error processing line {line_num}: {e}")
                    continue
        
        # Write remaining events
        if events_chunk:
            df_chunk = pd.DataFrame(events_chunk)
            if parquet_writer is None:
                # First and only chunk
                table = pa.Table.from_pandas(df_chunk)
                schema = table.schema
                parquet_writer = pq.ParquetWriter(
                    temp_path,
                    schema=schema,
                    compression="snappy",
                )
            table = pa.Table.from_pandas(df_chunk)
            # Cast to match original schema if needed
            if schema is not None:
                table = table.cast(schema)
            parquet_writer.write_table(table)
        
        # Close writer
        if parquet_writer is not None:
            parquet_writer.close()
        else:
            # No events found - create empty parquet file
            logger.warning(f"No valid events found in {input_gz_path}")
            df_empty = pd.DataFrame()
            df_empty.to_parquet(temp_path, engine="pyarrow", compression="snappy", index=False)
        
        # Atomic rename
        temp_path.replace(output_parquet_path)
        
        logger.info(f"Transformed OK: {output_parquet_path} ({total_events} events)")
        return str(output_parquet_path)
        
    except Exception as e:
        logger.error(f"Failed to transform {input_gz_path}: {e}")
        # Clean up temp file on error
        if temp_path.exists():
            temp_path.unlink()
        raise

