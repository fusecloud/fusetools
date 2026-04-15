"""Tests for transfer_tools module."""

import os
import tempfile

from fusetools.transfer_tools import Local


def test_zip_dir_creates_zip() -> None:
    """zip_dir should create a zip file from a list of directories."""
    with tempfile.TemporaryDirectory() as tmpdir:
        sub = os.path.join(tmpdir, "subdir")
        os.makedirs(sub)
        with open(os.path.join(sub, "file.txt"), "w") as f:
            f.write("hello")

        zipname = os.path.join(tmpdir, "out.zip")
        Local.zip_dir([sub], zipname)
        assert os.path.exists(zipname)
        assert os.path.getsize(zipname) > 0


def test_clear_delete_directory() -> None:
    """clear_delete_directory should remove directory contents."""
    with tempfile.TemporaryDirectory() as tmpdir:
        sub = os.path.join(tmpdir, "to_delete")
        os.makedirs(sub)
        with open(os.path.join(sub, "file.txt"), "w") as f:
            f.write("hello")

        Local.clear_delete_directory(sub, method="delete")
        assert not os.path.exists(sub)


def test_clear_directory_without_delete() -> None:
    """clear_delete_directory with method != delete should keep the directory."""
    with tempfile.TemporaryDirectory() as tmpdir:
        sub = os.path.join(tmpdir, "to_clear")
        os.makedirs(sub)
        with open(os.path.join(sub, "file.txt"), "w") as f:
            f.write("hello")

        Local.clear_delete_directory(sub, method="clear")
        assert os.path.exists(sub)
        assert len(os.listdir(sub)) == 0
