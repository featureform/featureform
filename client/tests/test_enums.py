import pytest
from featureform import FilePrefix


@pytest.mark.parametrize(
    "store_type, file_path",
    [
        ("AZURE", "abfss://test_bucket/test_file"),
        ("AZURE", "test_bucket/test_file"),
        ("S3", "s3://test_bucket/test_file"),
        ("S3", "test_bucket/test_file"),
        ("HDFS", "hdfs://test_bucket/test_file"),
        ("HDFS", "test_bucket/test_file"),
        ("GCS", "gs://test_bucket/test_file"),
        ("GCS", "test_bucket/test_file"),
    ],
)
def test_validate_file_scheme(store_type, file_path):
    try:
        FilePrefix.validate(store_type, file_path)
    except Exception as e:
        assert (
            str(e)
            == f"File path '{file_path}' must be a full path. Must start with '{FilePrefix[store_type].prefixes}'"
        )
