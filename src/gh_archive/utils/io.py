"""Atomic write helpers for safe file operations."""
import os
import shutil
import tempfile
from pathlib import Path
from typing import BinaryIO, TextIO


def atomic_write_binary(file_path: Path, content: bytes) -> None:
    """
    Atomically write binary content to a file.
    
    Uses a temporary file and rename to ensure atomicity.
    
    Args:
        file_path: Target file path
        content: Binary content to write
    """
    file_path = Path(file_path)
    file_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Write to temporary file in same directory
    temp_file = file_path.parent / f".{file_path.name}.tmp"
    
    try:
        with open(temp_file, "wb") as f:
            f.write(content)
        # Atomic rename
        temp_file.replace(file_path)
    except Exception:
        # Clean up temp file on error
        if temp_file.exists():
            temp_file.unlink()
        raise


def atomic_write_text(file_path: Path, content: str, encoding: str = "utf-8") -> None:
    """
    Atomically write text content to a file.
    
    Uses a temporary file and rename to ensure atomicity.
    
    Args:
        file_path: Target file path
        content: Text content to write
        encoding: Text encoding (default: utf-8)
    """
    atomic_write_binary(file_path, content.encode(encoding))


def atomic_copy(source: Path, destination: Path) -> None:
    """
    Atomically copy a file to destination.
    
    Args:
        source: Source file path
        destination: Destination file path
    """
    destination = Path(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)
    
    # Copy to temporary file first
    temp_file = destination.parent / f".{destination.name}.tmp"
    
    try:
        shutil.copy2(source, temp_file)
        # Atomic rename
        temp_file.replace(destination)
    except Exception:
        # Clean up temp file on error
        if temp_file.exists():
            temp_file.unlink()
        raise

