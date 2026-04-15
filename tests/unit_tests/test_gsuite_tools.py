"""Tests for gsuite_tools module."""


def test_import_gsuite_tools() -> None:
    """gsuite_tools should be importable without any optional deps installed."""
    from fusetools import gsuite_tools

    assert hasattr(gsuite_tools, "GSheets")
    assert hasattr(gsuite_tools, "GDrive")
    assert hasattr(gsuite_tools, "GMail")
    assert hasattr(gsuite_tools, "GDocs")
