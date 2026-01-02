"""Download GH Archive hourly data files."""
import logging
from pathlib import Path

import requests

logger = logging.getLogger(__name__)


def download_file(url: str, output_path: str, overwrite: bool = False) -> str:
    """
    Download a file from URL and save it to the specified path.
    
    Args:
        url: URL to download from
        output_path: Path where the file should be saved (directory will be created if needed)
        overwrite: If True, overwrite existing file. If False, skip if file exists.
    
    Returns:
        String path to the downloaded file
    """
    output_path = Path(output_path)
    
    # Create parent directory if it doesn't exist
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    if output_path.exists() and not overwrite:
        logger.info(f"File already exists: {output_path}")
        return str(output_path)

    logger.info(f"Downloading {url} -> {output_path}")

    tmp_path = output_path.parent / f".{output_path.name}.tmp"

    try:
        with requests.get(url, stream=True, timeout=300) as r:
            r.raise_for_status()
            with open(tmp_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=1024 * 1024):  # 1MB
                    if chunk:
                        f.write(chunk)

        # atomic replace
        tmp_path.replace(output_path)

        logger.info(f"Downloaded OK: {output_path}")
        return str(output_path)

    except Exception:
        # cleanup temp on failure
        if tmp_path.exists():
            try:
                tmp_path.unlink()
            except Exception:
                pass
        raise

