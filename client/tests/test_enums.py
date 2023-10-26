import pytest
from featureform import FilePrefix, FileFormat


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


@pytest.mark.parametrize(
    "location, expected_format",
    [
        ("s3://bucket/path/to/file.csv", "csv"),
        ("s3a://bucket/path/to/file.csv", "csv"),
        ("s3://bucket/path/to/directory/part-0000.parquet", "parquet"),
        ("s3://bucket/path/to/directory", "parquet"),
    ],
)
def test_get_format(location, expected_format):
    assert FileFormat.get_format(location, "parquet") == expected_format
