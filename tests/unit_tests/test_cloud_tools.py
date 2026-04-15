"""Tests for cloud_tools module."""


def test_import_cloud_tools() -> None:
    """cloud_tools should be importable without any optional deps installed."""
    from fusetools import cloud_tools

    assert hasattr(cloud_tools, "AWS")
    assert hasattr(cloud_tools, "GCP")


def test_aws_class_has_methods() -> None:
    """AWS class should expose expected methods."""
    from fusetools.cloud_tools import AWS

    expected = [
        "create_s3_bucket",
        "df_to_s3",
        "s3_to_df",
        "file_to_s3",
        "s3_to_file",
        "query_dynamo",
        "load_dynamo",
    ]
    for method in expected:
        assert hasattr(AWS, method), f"AWS missing method: {method}"
