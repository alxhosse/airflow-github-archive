"""Tests for gh_archive.jobs.stats module."""
import json
from pathlib import Path

import pandas as pd
import pytest

from gh_archive.jobs.stats import generate_stats


class TestGenerateStats:
    """Test cases for generate_stats function."""

    def _create_sample_parquet(self, path: Path, data: list[dict]):
        """Helper to create a sample Parquet file."""
        path.parent.mkdir(parents=True, exist_ok=True)
        df = pd.DataFrame(data)
        df.to_parquet(path, engine="pyarrow", compression="snappy", index=False)

    def test_successful_stats_generation(self, tmp_path):
        """Test successful generation of statistics."""
        parquet_path = tmp_path / "input.parquet"
        output_json = tmp_path / "stats.json"
        
        data = [
            {
                "id": "1",
                "type": "PushEvent",
                "actor_login": "alice",
                "repo_name": "alice/repo1",
            },
            {
                "id": "2",
                "type": "IssuesEvent",
                "actor_login": "bob",
                "repo_name": "bob/repo2",
            },
            {
                "id": "3",
                "type": "PushEvent",
                "actor_login": "alice",
                "repo_name": "alice/repo1",
            },
        ]
        self._create_sample_parquet(parquet_path, data)
        
        result = generate_stats(parquet_path, output_json)
        
        assert result == str(output_json)
        assert output_json.exists()
        
        # Verify JSON content
        with open(output_json) as f:
            stats = json.load(f)
        
        assert stats["total_events"] == 3
        assert stats["unique_actors"] == 2
        assert stats["unique_repos"] == 2
        assert "PushEvent" in stats["event_types"]
        assert stats["event_types"]["PushEvent"] == 2
        assert stats["event_types"]["IssuesEvent"] == 1

    def test_file_already_exists_no_overwrite(self, tmp_path):
        """Test that existing stats file is skipped when overwrite=False."""
        parquet_path = tmp_path / "input.parquet"
        output_json = tmp_path / "stats.json"
        
        # Create existing stats file
        existing_stats = {"total_events": 999}
        output_json.parent.mkdir(parents=True, exist_ok=True)
        with open(output_json, "w") as f:
            json.dump(existing_stats, f)
        
        data = [{"id": "1", "type": "PushEvent"}]
        self._create_sample_parquet(parquet_path, data)
        
        result = generate_stats(parquet_path, output_json, overwrite=False)
        
        assert result == str(output_json)
        # File should not have been overwritten
        with open(output_json) as f:
            stats = json.load(f)
        assert stats["total_events"] == 999

    def test_file_already_exists_with_overwrite(self, tmp_path):
        """Test that existing stats file is overwritten when overwrite=True."""
        parquet_path = tmp_path / "input.parquet"
        output_json = tmp_path / "stats.json"
        
        # Create existing stats file
        existing_stats = {"total_events": 999}
        output_json.parent.mkdir(parents=True, exist_ok=True)
        with open(output_json, "w") as f:
            json.dump(existing_stats, f)
        
        data = [{"id": "1", "type": "PushEvent"}]
        self._create_sample_parquet(parquet_path, data)
        
        result = generate_stats(parquet_path, output_json, overwrite=True)
        
        assert result == str(output_json)
        # File should have been overwritten
        with open(output_json) as f:
            stats = json.load(f)
        assert stats["total_events"] == 1

    def test_creates_parent_directories(self, tmp_path):
        """Test that parent directories are created if they don't exist."""
        parquet_path = tmp_path / "input.parquet"
        output_json = tmp_path / "nested" / "deep" / "path" / "stats.json"
        
        data = [{"id": "1", "type": "PushEvent"}]
        self._create_sample_parquet(parquet_path, data)
        
        result = generate_stats(parquet_path, output_json)
        
        assert result == str(output_json)
        assert output_json.exists()
        assert output_json.parent.exists()

    def test_handles_missing_parquet_file(self, tmp_path):
        """Test handling of missing Parquet file."""
        parquet_path = tmp_path / "nonexistent.parquet"
        output_json = tmp_path / "stats.json"
        
        result = generate_stats(parquet_path, output_json)
        
        assert result == str(output_json)
        assert output_json.exists()
        
        with open(output_json) as f:
            stats = json.load(f)
        
        assert stats["total_events"] == 0
        assert "error" in stats
        assert stats["error"] == "File not found"

    def test_handles_empty_parquet_file(self, tmp_path):
        """Test handling of empty Parquet file."""
        parquet_path = tmp_path / "input.parquet"
        output_json = tmp_path / "stats.json"
        
        # Create empty parquet file
        df = pd.DataFrame()
        parquet_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(parquet_path, engine="pyarrow", compression="snappy", index=False)
        
        result = generate_stats(parquet_path, output_json)
        
        assert result == str(output_json)
        with open(output_json) as f:
            stats = json.load(f)
        
        assert stats["total_events"] == 0

    def test_generates_top_event_types(self, tmp_path):
        """Test that top event types are included in stats."""
        parquet_path = tmp_path / "input.parquet"
        output_json = tmp_path / "stats.json"
        
        # Create data with multiple event types
        data = []
        for i in range(15):
            data.append({"id": str(i), "type": "PushEvent"})
        for i in range(10):
            data.append({"id": str(i + 15), "type": "IssuesEvent"})
        for i in range(5):
            data.append({"id": str(i + 25), "type": "PullRequestEvent"})
        
        self._create_sample_parquet(parquet_path, data)
        
        generate_stats(parquet_path, output_json)
        
        with open(output_json) as f:
            stats = json.load(f)
        
        assert "top_event_types" in stats
        assert stats["top_event_types"]["PushEvent"] == 15
        assert stats["top_event_types"]["IssuesEvent"] == 10
        # Should only include top 10
        assert len(stats["top_event_types"]) <= 10

    def test_generates_top_repos(self, tmp_path):
        """Test that top repositories are included in stats."""
        parquet_path = tmp_path / "input.parquet"
        output_json = tmp_path / "stats.json"
        
        data = []
        for i in range(20):
            data.append({"id": str(i), "type": "PushEvent", "repo_name": f"repo{i % 5}"})
        
        self._create_sample_parquet(parquet_path, data)
        
        generate_stats(parquet_path, output_json)
        
        with open(output_json) as f:
            stats = json.load(f)
        
        assert "top_repos" in stats
        assert len(stats["top_repos"]) <= 10

    def test_generates_top_actors(self, tmp_path):
        """Test that top actors are included in stats."""
        parquet_path = tmp_path / "input.parquet"
        output_json = tmp_path / "stats.json"
        
        data = []
        for i in range(20):
            data.append({"id": str(i), "type": "PushEvent", "actor_login": f"user{i % 5}"})
        
        self._create_sample_parquet(parquet_path, data)
        
        generate_stats(parquet_path, output_json)
        
        with open(output_json) as f:
            stats = json.load(f)
        
        assert "top_actors" in stats
        assert len(stats["top_actors"]) <= 10

    def test_handles_missing_columns(self, tmp_path):
        """Test handling of missing optional columns."""
        parquet_path = tmp_path / "input.parquet"
        output_json = tmp_path / "stats.json"
        
        # Create parquet with minimal columns
        data = [{"id": "1", "type": "PushEvent"}]
        self._create_sample_parquet(parquet_path, data)
        
        result = generate_stats(parquet_path, output_json)
        
        assert result == str(output_json)
        with open(output_json) as f:
            stats = json.load(f)
        
        assert stats["total_events"] == 1
        assert stats["unique_actors"] == 0  # actor_login column missing
        assert stats["unique_repos"] == 0  # repo_name column missing

    def test_handles_path_objects(self, tmp_path):
        """Test that function accepts Path objects."""
        parquet_path = tmp_path / "input.parquet"
        output_json = tmp_path / "stats.json"
        
        data = [{"id": "1", "type": "PushEvent"}]
        self._create_sample_parquet(parquet_path, data)
        
        # Pass Path objects instead of strings
        result = generate_stats(parquet_path, output_json)
        
        assert isinstance(result, str)
        assert output_json.exists()

    def test_handles_string_paths(self, tmp_path):
        """Test that function accepts string paths."""
        parquet_path = tmp_path / "input.parquet"
        output_json = tmp_path / "stats.json"
        
        data = [{"id": "1", "type": "PushEvent"}]
        self._create_sample_parquet(parquet_path, data)
        
        # Pass string paths
        result = generate_stats(str(parquet_path), str(output_json))
        
        assert isinstance(result, str)
        assert output_json.exists()

    def test_handles_error_during_processing(self, tmp_path):
        """Test handling of errors during Parquet reading."""
        parquet_path = tmp_path / "input.parquet"
        output_json = tmp_path / "stats.json"
        
        # Create invalid parquet file (empty file)
        parquet_path.parent.mkdir(parents=True, exist_ok=True)
        parquet_path.write_bytes(b"invalid parquet content")
        
        result = generate_stats(parquet_path, output_json)
        
        assert result == str(output_json)
        assert output_json.exists()
        
        with open(output_json) as f:
            stats = json.load(f)
        
        assert stats["total_events"] == 0
        assert "error" in stats

    def test_returns_string_path(self, tmp_path):
        """Test that function returns string path, not Path object."""
        parquet_path = tmp_path / "input.parquet"
        output_json = tmp_path / "stats.json"
        
        data = [{"id": "1", "type": "PushEvent"}]
        self._create_sample_parquet(parquet_path, data)
        
        result = generate_stats(parquet_path, output_json)
        
        assert isinstance(result, str)
        assert result == str(output_json)

    def test_stats_json_format(self, tmp_path):
        """Test that stats JSON is properly formatted."""
        parquet_path = tmp_path / "input.parquet"
        output_json = tmp_path / "stats.json"
        
        data = [
            {"id": "1", "type": "PushEvent", "actor_login": "alice", "repo_name": "alice/repo1"},
            {"id": "2", "type": "IssuesEvent", "actor_login": "bob", "repo_name": "bob/repo2"},
        ]
        self._create_sample_parquet(parquet_path, data)
        
        generate_stats(parquet_path, output_json)
        
        # Verify JSON is valid and properly formatted
        with open(output_json) as f:
            stats = json.load(f)
        
        # Check required fields
        assert "total_events" in stats
        assert "unique_actors" in stats
        assert "unique_repos" in stats
        assert "event_types" in stats
        
        # Verify JSON can be re-serialized
        json_str = json.dumps(stats, indent=2)
        assert len(json_str) > 0

