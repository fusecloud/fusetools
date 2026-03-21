"""Integration tests for cloud_tools — requires real AWS credentials."""

import os

import pytest
from dotenv import load_dotenv

load_dotenv()


@pytest.fixture
def aws_credentials() -> dict[str, str]:
    """Load AWS credentials from environment."""
    required = ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"]
    missing = [k for k in required if not os.environ.get(k)]
    if missing:
        pytest.skip(f"Missing env vars: {', '.join(missing)}")

    return {
        "pub": os.environ["AWS_ACCESS_KEY_ID"],
        "sec": os.environ["AWS_SECRET_ACCESS_KEY"],
        "region_name": os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
    }


def test_list_sqs_queues(aws_credentials: dict[str, str]) -> None:
    """Test listing SQS queues with real credentials."""
    from fusetools.cloud_tools import AWS

    result = AWS.list_sqs_queues(**aws_credentials, max_results=1)
    assert result is not None
