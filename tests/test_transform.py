"""Tests for gh_archive.jobs.transform module."""
import gzip
import json
from pathlib import Path

import pandas as pd
import pytest

from gh_archive.jobs.transform import extract_important_columns, transform_json_to_parquet


class TestExtractImportantColumns:
    """Test cases for extract_important_columns function."""

    def test_extract_basic_fields(self):
        """Test extraction of basic event fields."""
        event = {
            "id": "123",
            "type": "PushEvent",
            "created_at": "2024-01-01T00:00:00Z",
            "public": True,
        }
        result = extract_important_columns(event)
        
        assert result["id"] == "123"
        assert result["type"] == "PushEvent"
        assert result["created_at"] == "2024-01-01T00:00:00Z"
        assert result["public"] is True

    def test_extract_actor_info(self):
        """Test extraction of actor information."""
        event = {
            "id": "123",
            "type": "PushEvent",
            "actor": {"id": 10, "login": "alice", "type": "User"},
        }
        result = extract_important_columns(event)
        
        assert result["actor_id"] == 10
        assert result["actor_login"] == "alice"
        assert result["actor_type"] == "User"

    def test_extract_repo_info(self):
        """Test extraction of repository information."""
        event = {
            "id": "123",
            "type": "PushEvent",
            "repo": {"id": 100, "name": "alice/repo1", "url": "https://github.com/alice/repo1"},
        }
        result = extract_important_columns(event)
        
        assert result["repo_id"] == 100
        assert result["repo_name"] == "alice/repo1"
        assert result["repo_url"] == "https://github.com/alice/repo1"

    def test_extract_org_info(self):
        """Test extraction of organization information."""
        event = {
            "id": "123",
            "type": "PushEvent",
            "org": {"id": 5, "login": "myorg"},
        }
        result = extract_important_columns(event)
        
        assert result["org_id"] == 5
        assert result["org_login"] == "myorg"

    def test_extract_pushevent_payload(self):
        """Test extraction of PushEvent-specific payload fields."""
        event = {
            "id": "123",
            "type": "PushEvent",
            "payload": {"size": 3, "distinct_size": 2},
        }
        result = extract_important_columns(event)
        
        assert result["payload_size"] == 3
        assert result["payload_distinct_size"] == 2

    def test_extract_other_event_payload(self):
        """Test extraction of payload for non-PushEvent."""
        event = {
            "id": "123",
            "type": "IssuesEvent",
            "payload": {"action": "opened"},
        }
        result = extract_important_columns(event)
        
        assert result["payload_action"] == "opened"
        assert result["payload_size"] is None
        assert result["payload_distinct_size"] is None

    def test_handle_missing_fields(self):
        """Test handling of missing optional fields."""
        event = {"id": "123", "type": "PushEvent"}
        result = extract_important_columns(event)
        
        assert result["id"] == "123"
        assert result["type"] == "PushEvent"
        assert result["actor_id"] is None
        assert result["repo_id"] is None
        assert result["org_id"] is None

    def test_handle_non_dict_actor(self):
        """Test handling of non-dict actor field."""
        event = {"id": "123", "type": "PushEvent", "actor": None}
        result = extract_important_columns(event)
        
        assert result["actor_id"] is None
        assert result["actor_login"] is None
        assert result["actor_type"] is None


class TestTransformJsonToParquet:
    """Test cases for transform_json_to_parquet function."""

    def _create_sample_json_gz(self, path: Path, events: list[dict]):
        """Helper to create a sample JSON.gz file."""
        path.parent.mkdir(parents=True, exist_ok=True)
        with gzip.open(path, "wt", encoding="utf-8") as f:
            for event in events:
                f.write(json.dumps(event) + "\n")

    def test_successful_transform(self, tmp_path):
        """Test successful transformation of JSON.gz to Parquet."""
        input_gz = tmp_path / "input.json.gz"
        output_parquet = tmp_path / "output.parquet"
        
        events = [
            {
                "id": "1",
                "type": "PushEvent",
                "created_at": "2024-01-01T00:00:00Z",
                "public": True,
                "actor": {"id": 10, "login": "alice", "type": "User"},
                "repo": {"id": 100, "name": "alice/repo1", "url": "https://github.com/alice/repo1"},
                "payload": {"size": 2, "distinct_size": 2},
            },
            {
                "id": "2",
                "type": "IssuesEvent",
                "created_at": "2024-01-01T00:00:10Z",
                "public": True,
                "actor": {"id": 20, "login": "bob", "type": "User"},
                "repo": {"id": 200, "name": "bob/repo2", "url": "https://github.com/bob/repo2"},
                "payload": {"action": "opened"},
            },
        ]
        
        self._create_sample_json_gz(input_gz, events)
        
        result = transform_json_to_parquet(input_gz, output_parquet)
        
        assert result == str(output_parquet)
        assert output_parquet.exists()
        
        # Verify Parquet content
        df = pd.read_parquet(output_parquet)
        assert len(df) == 2
        assert "type" in df.columns
        assert "actor_login" in df.columns
        assert "repo_name" in df.columns
        assert set(df["type"].tolist()) == {"PushEvent", "IssuesEvent"}

    def test_file_already_exists_no_overwrite(self, tmp_path):
        """Test that existing Parquet file is skipped when overwrite=False."""
        input_gz = tmp_path / "input.json.gz"
        output_parquet = tmp_path / "output.parquet"
        
        # Create existing parquet file
        df_existing = pd.DataFrame({"test": [1, 2, 3]})
        df_existing.to_parquet(output_parquet)
        original_size = output_parquet.stat().st_size
        
        events = [{"id": "1", "type": "PushEvent"}]
        self._create_sample_json_gz(input_gz, events)
        
        result = transform_json_to_parquet(input_gz, output_parquet, overwrite=False)
        
        assert result == str(output_parquet)
        # File should not have been overwritten
        assert output_parquet.stat().st_size == original_size

    def test_file_already_exists_with_overwrite(self, tmp_path):
        """Test that existing Parquet file is overwritten when overwrite=True."""
        input_gz = tmp_path / "input.json.gz"
        output_parquet = tmp_path / "output.parquet"
        
        # Create existing parquet file
        df_existing = pd.DataFrame({"test": [1, 2, 3]})
        df_existing.to_parquet(output_parquet)
        
        events = [{"id": "1", "type": "PushEvent"}]
        self._create_sample_json_gz(input_gz, events)
        
        result = transform_json_to_parquet(input_gz, output_parquet, overwrite=True)
        
        assert result == str(output_parquet)
        # File should have been overwritten
        df = pd.read_parquet(output_parquet)
        assert "type" in df.columns
        assert "test" not in df.columns

    def test_creates_parent_directories(self, tmp_path):
        """Test that parent directories are created if they don't exist."""
        input_gz = tmp_path / "input.json.gz"
        output_parquet = tmp_path / "nested" / "deep" / "path" / "output.parquet"
        
        events = [{"id": "1", "type": "PushEvent"}]
        self._create_sample_json_gz(input_gz, events)
        
        result = transform_json_to_parquet(input_gz, output_parquet)
        
        assert result == str(output_parquet)
        assert output_parquet.exists()
        assert output_parquet.parent.exists()

    def test_handles_empty_file(self, tmp_path):
        """Test transformation of empty JSON.gz file."""
        input_gz = tmp_path / "input.json.gz"
        output_parquet = tmp_path / "output.parquet"
        
        # Create empty gz file
        input_gz.parent.mkdir(parents=True, exist_ok=True)
        with gzip.open(input_gz, "wt", encoding="utf-8") as f:
            pass  # Empty file
        
        result = transform_json_to_parquet(input_gz, output_parquet)
        
        assert result == str(output_parquet)
        assert output_parquet.exists()
        
        df = pd.read_parquet(output_parquet)
        assert len(df) == 0

    def test_handles_invalid_json_lines(self, tmp_path):
        """Test that invalid JSON lines are skipped."""
        input_gz = tmp_path / "input.json.gz"
        output_parquet = tmp_path / "output.parquet"
        
        input_gz.parent.mkdir(parents=True, exist_ok=True)
        with gzip.open(input_gz, "wt", encoding="utf-8") as f:
            f.write('{"id": "1", "type": "PushEvent"}\n')
            f.write("invalid json line\n")
            f.write('{"id": "2", "type": "IssuesEvent"}\n')
        
        result = transform_json_to_parquet(input_gz, output_parquet)
        
        assert result == str(output_parquet)
        df = pd.read_parquet(output_parquet)
        # Should have 2 valid events
        assert len(df) == 2

    def test_handles_large_file_chunked(self, tmp_path):
        """Test that large files are processed in chunks."""
        input_gz = tmp_path / "input.json.gz"
        output_parquet = tmp_path / "output.parquet"
        
        # Create file with more than CHUNK_SIZE events (10000)
        events = [
            {"id": str(i), "type": "PushEvent", "actor": {"login": f"user{i}"}}
            for i in range(15000)
        ]
        self._create_sample_json_gz(input_gz, events)
        
        result = transform_json_to_parquet(input_gz, output_parquet)
        
        assert result == str(output_parquet)
        df = pd.read_parquet(output_parquet)
        assert len(df) == 15000

    def test_handles_path_objects(self, tmp_path):
        """Test that function accepts Path objects."""
        input_gz = tmp_path / "input.json.gz"
        output_parquet = tmp_path / "output.parquet"
        
        events = [{"id": "1", "type": "PushEvent"}]
        self._create_sample_json_gz(input_gz, events)
        
        # Pass Path objects instead of strings
        result = transform_json_to_parquet(input_gz, output_parquet)
        
        assert isinstance(result, str)
        assert output_parquet.exists()

    def test_handles_string_paths(self, tmp_path):
        """Test that function accepts string paths."""
        input_gz = tmp_path / "input.json.gz"
        output_parquet = tmp_path / "output.parquet"
        
        events = [{"id": "1", "type": "PushEvent"}]
        self._create_sample_json_gz(input_gz, events)
        
        # Pass string paths
        result = transform_json_to_parquet(str(input_gz), str(output_parquet))
        
        assert isinstance(result, str)
        assert output_parquet.exists()

    def test_atomic_write_using_temp_file(self, tmp_path):
        """Test that file is written atomically via temp file."""
        input_gz = tmp_path / "input.json.gz"
        output_parquet = tmp_path / "output.parquet"
        
        events = [{"id": "1", "type": "PushEvent"}]
        self._create_sample_json_gz(input_gz, events)
        
        transform_json_to_parquet(input_gz, output_parquet)
        
        # Verify temp file doesn't exist after successful transform
        temp_file = output_parquet.parent / f".{output_parquet.name}.tmp"
        assert not temp_file.exists()
        assert output_parquet.exists()

    def test_cleanup_temp_file_on_error(self, tmp_path):
        """Test that temp file is cleaned up on error."""
        input_gz = tmp_path / "nonexistent.json.gz"
        output_parquet = tmp_path / "output.parquet"
        
        with pytest.raises(Exception):
            transform_json_to_parquet(input_gz, output_parquet)
        
        # Temp file should be cleaned up
        temp_file = output_parquet.parent / f".{output_parquet.name}.tmp"
        assert not temp_file.exists()
        assert not output_parquet.exists()

    def test_returns_string_path(self, tmp_path):
        """Test that function returns string path, not Path object."""
        input_gz = tmp_path / "input.json.gz"
        output_parquet = tmp_path / "output.parquet"
        
        events = [{"id": "1", "type": "PushEvent"}]
        self._create_sample_json_gz(input_gz, events)
        
        result = transform_json_to_parquet(input_gz, output_parquet)
        
        assert isinstance(result, str)
        assert result == str(output_parquet)

