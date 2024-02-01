import pytest

from featureform.resources import DatabricksCredentials, EMRCredentials, AWSCredentials


@pytest.mark.parametrize(
    "username,password,host,token,cluster_id",
    [
        # either use username-password pair or host-token pair
        (
            "",
            "",
            "host_xyz",
            "dapiabcdefghijklmnopqrstuvwxyz123456-6",
            "abcd-123def-ghijklmn",
        ),
        # valid token and cluster id
        pytest.param(
            "john",
            "abc123",
            "host_xyz",
            "dapiabcdefghijklmnopqrstuvwxyz12345-6",
            "abcd-123def-ghijklmn",
            marks=pytest.mark.xfail,
        ),
        # valid token w/o hyphenated suffix and cluster id
        pytest.param(
            "",
            "",
            "host_xyz",
            "dapiabcdefghijklmnopqrstuvwxyz123456",
            "abcd-123def-ghijklmn",
        ),
        # cluster id should always be provided
        ("john", "abc123", "", "", "abcd-123def-ghijklmn"),
        pytest.param(
            "john",
            "abc123",
            "",
            "",
            "",
            marks=pytest.mark.xfail,
        ),
        # token and cluster id are empty
        pytest.param("", "", "a", "", "", marks=pytest.mark.xfail),  # invalid case
        # invalid token id
        pytest.param(
            "",
            "",
            "host_xyz",
            "dainvalid_token",
            "abcd-123def-ghijklmn",
            marks=pytest.mark.xfail,
        ),
        pytest.param(
            "",
            "",
            "host_xyz",
            "dainvalid#$%_token!$%",
            "abcd-123def-ghijklmn",
            marks=pytest.mark.xfail,
        ),
        # invalid cluster id
        pytest.param(
            "",
            "",
            "host_xyz",
            "dapiabcdefghijklmnopqrstuvwxyz12345-6",
            "abc-1234def-ghijklmnopq",
            marks=pytest.mark.xfail,
        ),
        pytest.param(
            "",
            "",
            "host_xyz",
            "dapiabcdefghijklmnopqrstuvwxyz12345-6",
            "abc-!@#4def-ghijklmnopq",
            marks=pytest.mark.xfail,
        ),
        pytest.param("", "", "", "", "abcd-123def-ghijklmn", marks=pytest.mark.xfail),
    ],
)
def test_databricks_credentials(username, password, host, token, cluster_id):
    databricks = DatabricksCredentials(
        username=username,
        password=password,
        host=host,
        token=token,
        cluster_id=cluster_id,
    )

    expected_config = {
        "Username": username,
        "Password": password,
        "Host": host,
        "Token": token,
        "Cluster": cluster_id,
    }

    assert databricks.type() == "DATABRICKS"
    assert databricks.config() == expected_config


@pytest.mark.parametrize(
    "aws_access_key_id,aws_secret_access_key,emr_cluster_id,emr_cluster_region",
    [
        ("a", "b", "c", "d"),
        pytest.param("", "", "a", "b", marks=pytest.mark.xfail),
        pytest.param("", "", "", "", marks=pytest.mark.xfail),
    ],
)
def test_emr_credentials(
    aws_access_key_id, aws_secret_access_key, emr_cluster_id, emr_cluster_region
):
    emr = EMRCredentials(
        emr_cluster_id=emr_cluster_id,
        emr_cluster_region=emr_cluster_region,
        credentials=AWSCredentials(
            access_key=aws_access_key_id, secret_key=aws_secret_access_key
        ),
    )

    expected_config = {
        "ClusterName": emr_cluster_id,
        "ClusterRegion": emr_cluster_region,
        "Credentials": {"AWSAccessKeyId": "a", "AWSSecretKey": "b"},
    }

    assert emr.type() == "EMR"
    assert emr.config() == expected_config
