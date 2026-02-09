import os
import tempfile

import pytest

from bump_version import (
    get_current_version,
    bump_patch_version,
    update_version_in_file,
    write_github_output,
)


class TestGetCurrentVersion:
    def test_reads_version_with_double_quotes(self, tmp_path):
        setup_file = tmp_path / "setup.py"
        setup_file.write_text('version="1.2.3"')

        assert get_current_version(str(setup_file)) == "1.2.3"

    def test_reads_version_with_single_quotes(self, tmp_path):
        setup_file = tmp_path / "setup.py"
        setup_file.write_text("version='0.1.0'")

        assert get_current_version(str(setup_file)) == "0.1.0"

    def test_reads_version_with_spaces_around_equals(self, tmp_path):
        setup_file = tmp_path / "setup.py"
        setup_file.write_text('version = "2.0.5"')

        assert get_current_version(str(setup_file)) == "2.0.5"

    def test_reads_version_in_full_setup_file(self, tmp_path):
        setup_file = tmp_path / "setup.py"
        content = """
from setuptools import setup

setup(
    name="my-package",
    version="3.4.5",
    description="Test package",
)
"""
        setup_file.write_text(content)

        assert get_current_version(str(setup_file)) == "3.4.5"

    def test_raises_error_when_version_not_found(self, tmp_path):
        setup_file = tmp_path / "setup.py"
        setup_file.write_text('name="my-package"')

        with pytest.raises(ValueError, match="Could not find version"):
            get_current_version(str(setup_file))

    def test_raises_error_for_invalid_version_format(self, tmp_path):
        setup_file = tmp_path / "setup.py"
        setup_file.write_text('version="1.2"')

        with pytest.raises(ValueError, match="Could not find version"):
            get_current_version(str(setup_file))


class TestBumpPatchVersion:
    def test_increments_patch_version(self):
        assert bump_patch_version("0.1.0") == "0.1.1"

    def test_increments_from_non_zero_patch(self):
        assert bump_patch_version("1.2.9") == "1.2.10"

    def test_preserves_major_and_minor(self):
        assert bump_patch_version("5.10.3") == "5.10.4"

    def test_handles_large_patch_numbers(self):
        assert bump_patch_version("1.0.99") == "1.0.100"


class TestUpdateVersionInFile:
    def test_updates_version_in_file(self, tmp_path):
        setup_file = tmp_path / "setup.py"
        setup_file.write_text('version="0.1.0"')

        update_version_in_file(str(setup_file), "0.1.0", "0.1.1")

        assert setup_file.read_text() == 'version="0.1.1"'

    def test_preserves_surrounding_content(self, tmp_path):
        setup_file = tmp_path / "setup.py"
        content = """from setuptools import setup

setup(
    name="my-package",
    version="1.0.0",
    description="Test",
)
"""
        setup_file.write_text(content)

        update_version_in_file(str(setup_file), "1.0.0", "1.0.1")

        result = setup_file.read_text()
        assert 'version="1.0.1"' in result
        assert 'name="my-package"' in result
        assert 'description="Test"' in result

    def test_returns_new_version(self, tmp_path):
        setup_file = tmp_path / "setup.py"
        setup_file.write_text('version="0.1.0"')

        result = update_version_in_file(str(setup_file), "0.1.0", "0.1.1")

        assert result == "0.1.1"


class TestWriteGithubOutput:
    def test_writes_to_github_output_file(self, tmp_path, monkeypatch):
        output_file = tmp_path / "github_output"
        output_file.write_text("")
        monkeypatch.setenv("GITHUB_OUTPUT", str(output_file))

        write_github_output("1.2.3", "1.2.2")

        content = output_file.read_text()
        assert "version=1.2.3\n" in content
        assert "old_version=1.2.2\n" in content

    def test_appends_to_existing_content(self, tmp_path, monkeypatch):
        output_file = tmp_path / "github_output"
        output_file.write_text("existing=value\n")
        monkeypatch.setenv("GITHUB_OUTPUT", str(output_file))

        write_github_output("2.0.0", "1.9.9")

        content = output_file.read_text()
        assert content.startswith("existing=value\n")
        assert "version=2.0.0\n" in content
        assert "old_version=1.9.9\n" in content

    def test_does_nothing_when_github_output_not_set(self, monkeypatch):
        monkeypatch.delenv("GITHUB_OUTPUT", raising=False)

        write_github_output("1.0.0", "0.9.9")
