"""Tests for gh_archive.jobs.fetch module."""
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

import pytest
import requests

from gh_archive.jobs.fetch import download_file


class TestDownloadFile:
    """Test cases for download_file function."""

    def test_successful_download(self, tmp_path):
        """Test successful file download."""
        url = "https://example.com/test.json.gz"
        output_path = tmp_path / "test.json.gz"
        
        # Mock successful HTTP response
        mock_response = MagicMock()
        mock_response.__enter__ = Mock(return_value=mock_response)
        mock_response.__exit__ = Mock(return_value=None)
        mock_response.raise_for_status = Mock()
        mock_response.iter_content.return_value = [b"chunk1", b"chunk2", b"chunk3"]
        
        with patch("gh_archive.jobs.fetch.requests.get", return_value=mock_response):
            result = download_file(url, str(output_path))
        
        assert result == str(output_path)
        assert output_path.exists()
        assert output_path.read_bytes() == b"chunk1chunk2chunk3"
        mock_response.raise_for_status.assert_called_once()

    def test_file_already_exists_no_overwrite(self, tmp_path):
        """Test that existing file is skipped when overwrite=False."""
        url = "https://example.com/test.json.gz"
        output_path = tmp_path / "test.json.gz"
        output_path.write_text("existing content")
        
        with patch("gh_archive.jobs.fetch.requests.get") as mock_get:
            result = download_file(url, str(output_path), overwrite=False)
        
        assert result == str(output_path)
        assert output_path.read_text() == "existing content"
        mock_get.assert_not_called()

    def test_file_already_exists_with_overwrite(self, tmp_path):
        """Test that existing file is overwritten when overwrite=True."""
        url = "https://example.com/test.json.gz"
        output_path = tmp_path / "test.json.gz"
        output_path.write_text("old content")
        
        # Mock successful HTTP response
        mock_response = MagicMock()
        mock_response.__enter__ = Mock(return_value=mock_response)
        mock_response.__exit__ = Mock(return_value=None)
        mock_response.raise_for_status = Mock()
        mock_response.iter_content.return_value = [b"new content"]
        
        with patch("gh_archive.jobs.fetch.requests.get", return_value=mock_response):
            result = download_file(url, str(output_path), overwrite=True)
        
        assert result == str(output_path)
        assert output_path.read_bytes() == b"new content"

    def test_creates_parent_directories(self, tmp_path):
        """Test that parent directories are created if they don't exist."""
        url = "https://example.com/test.json.gz"
        output_path = tmp_path / "nested" / "deep" / "path" / "test.json.gz"
        
        # Mock successful HTTP response
        mock_response = MagicMock()
        mock_response.__enter__ = Mock(return_value=mock_response)
        mock_response.__exit__ = Mock(return_value=None)
        mock_response.raise_for_status = Mock()
        mock_response.iter_content.return_value = [b"content"]
        
        with patch("gh_archive.jobs.fetch.requests.get", return_value=mock_response):
            result = download_file(url, str(output_path))
        
        assert result == str(output_path)
        assert output_path.exists()
        assert output_path.parent.exists()

    def test_http_error_raises_exception(self, tmp_path):
        """Test that HTTP errors are raised."""
        url = "https://example.com/test.json.gz"
        output_path = tmp_path / "test.json.gz"
        
        # Mock HTTP error response
        mock_response = MagicMock()
        mock_response.__enter__ = Mock(return_value=mock_response)
        mock_response.__exit__ = Mock(return_value=None)
        mock_response.raise_for_status.side_effect = requests.HTTPError("404 Not Found")
        
        with patch("gh_archive.jobs.fetch.requests.get", return_value=mock_response):
            with pytest.raises(requests.HTTPError):
                download_file(url, str(output_path))
        
        # Temp file should be cleaned up
        tmp_file = output_path.parent / f".{output_path.name}.tmp"
        assert not tmp_file.exists()
        assert not output_path.exists()

    def test_connection_error_raises_exception(self, tmp_path):
        """Test that connection errors are raised."""
        url = "https://example.com/test.json.gz"
        output_path = tmp_path / "test.json.gz"
        
        with patch("gh_archive.jobs.fetch.requests.get", side_effect=requests.ConnectionError("Connection failed")):
            with pytest.raises(requests.ConnectionError):
                download_file(url, str(output_path))
        
        # Temp file should be cleaned up
        tmp_file = output_path.parent / f".{output_path.name}.tmp"
        assert not tmp_file.exists()
        assert not output_path.exists()

    def test_timeout_error_raises_exception(self, tmp_path):
        """Test that timeout errors are raised."""
        url = "https://example.com/test.json.gz"
        output_path = tmp_path / "test.json.gz"
        
        with patch("gh_archive.jobs.fetch.requests.get", side_effect=requests.Timeout("Request timeout")):
            with pytest.raises(requests.Timeout):
                download_file(url, str(output_path))
        
        # Temp file should be cleaned up
        tmp_file = output_path.parent / f".{output_path.name}.tmp"
        assert not tmp_file.exists()
        assert not output_path.exists()

    def test_atomic_write_using_temp_file(self, tmp_path):
        """Test that file is written atomically via temp file."""
        url = "https://example.com/test.json.gz"
        output_path = tmp_path / "test.json.gz"
        
        # Mock successful HTTP response
        mock_response = MagicMock()
        mock_response.__enter__ = Mock(return_value=mock_response)
        mock_response.__exit__ = Mock(return_value=None)
        mock_response.raise_for_status = Mock()
        mock_response.iter_content.return_value = [b"content"]
        
        with patch("gh_archive.jobs.fetch.requests.get", return_value=mock_response):
            download_file(url, str(output_path))
        
        # Verify temp file doesn't exist after successful download
        tmp_file = output_path.parent / f".{output_path.name}.tmp"
        assert not tmp_file.exists()
        assert output_path.exists()

    def test_large_file_chunked_download(self, tmp_path):
        """Test that large files are downloaded in chunks."""
        url = "https://example.com/large.json.gz"
        output_path = tmp_path / "large.json.gz"
        
        # Create large content (simulate 5MB file)
        large_chunks = [b"x" * (1024 * 1024)] * 5  # 5 chunks of 1MB each
        
        # Mock successful HTTP response
        mock_response = MagicMock()
        mock_response.__enter__ = Mock(return_value=mock_response)
        mock_response.__exit__ = Mock(return_value=None)
        mock_response.raise_for_status = Mock()
        mock_response.iter_content.return_value = large_chunks
        
        with patch("gh_archive.jobs.fetch.requests.get", return_value=mock_response):
            result = download_file(url, str(output_path))
        
        assert result == str(output_path)
        assert output_path.exists()
        assert len(output_path.read_bytes()) == 5 * 1024 * 1024

    def test_empty_file_download(self, tmp_path):
        """Test downloading an empty file."""
        url = "https://example.com/empty.json.gz"
        output_path = tmp_path / "empty.json.gz"
        
        # Mock successful HTTP response with no content
        mock_response = MagicMock()
        mock_response.__enter__ = Mock(return_value=mock_response)
        mock_response.__exit__ = Mock(return_value=None)
        mock_response.raise_for_status = Mock()
        mock_response.iter_content.return_value = []
        
        with patch("gh_archive.jobs.fetch.requests.get", return_value=mock_response):
            result = download_file(url, str(output_path))
        
        assert result == str(output_path)
        assert output_path.exists()
        assert len(output_path.read_bytes()) == 0

    def test_skips_empty_chunks(self, tmp_path):
        """Test that empty chunks are skipped during download."""
        url = "https://example.com/test.json.gz"
        output_path = tmp_path / "test.json.gz"
        
        # Mock response with some empty chunks
        mock_response = MagicMock()
        mock_response.__enter__ = Mock(return_value=mock_response)
        mock_response.__exit__ = Mock(return_value=None)
        mock_response.raise_for_status = Mock()
        mock_response.iter_content.return_value = [b"chunk1", b"", b"chunk2", b"", b"chunk3"]
        
        with patch("gh_archive.jobs.fetch.requests.get", return_value=mock_response):
            result = download_file(url, str(output_path))
        
        assert result == str(output_path)
        assert output_path.read_bytes() == b"chunk1chunk2chunk3"

    def test_temp_file_cleanup_on_write_error(self, tmp_path):
        """Test that temp file is cleaned up if write fails."""
        url = "https://example.com/test.json.gz"
        output_path = tmp_path / "test.json.gz"
        
        # Mock response that raises error during write
        mock_response = MagicMock()
        mock_response.__enter__ = Mock(return_value=mock_response)
        mock_response.__exit__ = Mock(return_value=None)
        mock_response.raise_for_status = Mock()
        
        # Simulate write error
        def failing_iter_content(chunk_size):
            yield b"chunk1"
            raise IOError("Disk full")
        
        mock_response.iter_content.return_value = failing_iter_content(1024 * 1024)
        
        with patch("gh_archive.jobs.fetch.requests.get", return_value=mock_response):
            with pytest.raises(IOError):
                download_file(url, str(output_path))
        
        # Temp file should be cleaned up
        tmp_file = output_path.parent / f".{output_path.name}.tmp"
        assert not tmp_file.exists()
        assert not output_path.exists()

    def test_returns_string_path(self, tmp_path):
        """Test that function returns string path, not Path object."""
        url = "https://example.com/test.json.gz"
        output_path = tmp_path / "test.json.gz"
        
        # Mock successful HTTP response
        mock_response = MagicMock()
        mock_response.__enter__ = Mock(return_value=mock_response)
        mock_response.__exit__ = Mock(return_value=None)
        mock_response.raise_for_status = Mock()
        mock_response.iter_content.return_value = [b"content"]
        
        with patch("gh_archive.jobs.fetch.requests.get", return_value=mock_response):
            result = download_file(url, str(output_path))
        
        assert isinstance(result, str)
        assert result == str(output_path)

    def test_handles_path_object_input(self, tmp_path):
        """Test that function accepts Path object as input."""
        url = "https://example.com/test.json.gz"
        output_path = tmp_path / "test.json.gz"
        
        # Mock successful HTTP response
        mock_response = MagicMock()
        mock_response.__enter__ = Mock(return_value=mock_response)
        mock_response.__exit__ = Mock(return_value=None)
        mock_response.raise_for_status = Mock()
        mock_response.iter_content.return_value = [b"content"]
        
        with patch("gh_archive.jobs.fetch.requests.get", return_value=mock_response):
            # Pass Path object instead of string
            result = download_file(url, output_path)
        
        assert isinstance(result, str)
        assert result == str(output_path)
        assert output_path.exists()

