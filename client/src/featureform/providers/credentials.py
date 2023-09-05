from typeguard import typechecked
from dataclasses import dataclass
from typing import Optional, Dict, Union

import json
import os
import sys

# Constants for Pyspark Versions
MAJOR_VERSION = "3"
MINOR_VERSIONS = ["7", "8", "9", "10", "11"]


@typechecked
@dataclass
class AWSCredentials:
    """
    AWS Credentials for accessing AWS Services

    Attributes:
        access_key (str): AWS Access Key ID
        secret_key (str): AWS Secret Access Key
    """

    def __init__(
        self,
        access_key: str = "",
        secret_key: str = "",
    ):
        if access_key == "":
            raise Exception("'AWSCredentials' access_key cannot be empty")

        if secret_key == "":
            raise Exception("'AWSCredentials' secret_key cannot be empty")

        self.access_key = access_key
        self.secret_key = secret_key

    def type(self):
        return "AWS_CREDENTIALS"

    def config(self):
        return {
            "AWSAccessKeyId": self.access_key,
            "AWSSecretKey": self.secret_key,
        }


@typechecked
@dataclass
class GCPCredentials:
    """
    GCP Credentials for accessing GCP Services

    Attributes:
        project_id (str): GCP Project ID
        credentials_path (str): Path to GCP Credentials JSON file
    """

    def __init__(
        self,
        project_id: str,
        credentials_path: str,
        json_creds: Optional[Dict] = None,
    ):
        if project_id == "":
            raise Exception("'GCPCredentials' project_id cannot be empty")

        if credentials_path == "" and json_creds is None:
            raise Exception(
                "'GCPCredentials' credentials_path cannot be empty or credentials cannot be None"
            )

        self.project_id = project_id
        self.credentials_path = credentials_path
        self.json_creds = json_creds
        if credentials_path != "" and json_creds is None:
            if not os.path.isfile(self.credentials_path):
                raise Exception(
                    f"'GCPCredentials' credentials_path '{self.credentials_path}' file not found"
                )
            with open(self.credentials_path) as f:
                self.json_creds = json.load(f)
        else:
            self.json_creds = json_creds

    def type(self):
        return "GCPCredentials"

    def config(self):
        return {
            "ProjectId": self.project_id,
            "JSON": self.json_creds,
        }


## Executor Providers
@typechecked
class DatabricksCredentials:
    def __init__(
        self,
        username: str = "",
        password: str = "",
        host: str = "",
        token: str = "",
        cluster_id: str = "",
    ):
        self.username = username
        self.password = password
        self.host = host
        self.token = token
        self.cluster_id = cluster_id

        host_token_provided = (
            username == "" and password == "" and host != "" and token != ""
        )
        username_password_provided = (
            username != "" and password != "" and host == "" and token == ""
        )

        if (
            not host_token_provided
            and not username_password_provided
            or host_token_provided
            and username_password_provided
        ):
            raise Exception(
                "'DatabricksCredentials' requires either 'username' and 'password' or 'host' and 'token' to be set"
            )

        if not cluster_id:
            raise Exception("Cluster_id of existing cluster must be provided")

    def type(self):
        return "DATABRICKS"

    def config(self):
        return {
            "Username": self.username,
            "Password": self.password,
            "Host": self.host,
            "Token": self.token,
            "Cluster": self.cluster_id,
        }


@typechecked
@dataclass
class EMRCredentials:
    def __init__(
        self, emr_cluster_id: str, emr_cluster_region: str, credentials: AWSCredentials
    ):
        self.emr_cluster_id = emr_cluster_id
        self.emr_cluster_region = emr_cluster_region
        self.credentials = credentials

        if self.emr_cluster_id == "":
            raise Exception("'EMRCredentials' emr_cluster_id cannot be empty")
        if self.emr_cluster_region == "":
            raise Exception("'EMRCredentials' emr_cluster_region cannot be empty")

    def type(self):
        return "EMR"

    def config(self):
        return {
            "ClusterName": self.emr_cluster_id,
            "ClusterRegion": self.emr_cluster_region,
            "Credentials": self.credentials.config(),
        }


@typechecked
@dataclass
class SparkCredentials:
    def __init__(
        self,
        master: str,
        deploy_mode: str,
        python_version: str = "",
        core_site_path: str = "",
        yarn_site_path: str = "",
    ):
        self.master = master.lower()
        self.deploy_mode = deploy_mode.lower()
        self.core_site_path = core_site_path
        self.yarn_site_path = yarn_site_path

        if self.deploy_mode != "cluster" and self.deploy_mode != "client":
            raise Exception(
                f"Spark does not support '{self.deploy_mode}' deploy mode. It only supports 'cluster' and 'client'."
            )

        self.python_version = self._verify_python_version(
            self.deploy_mode, python_version
        )

        self._verify_yarn_config()

    def _verify_python_version(self, deploy_mode, version):
        if deploy_mode == "cluster" and version == "":
            client_python_version = sys.version_info
            client_major = str(client_python_version.major)
            client_minor = str(client_python_version.minor)

            if client_major != MAJOR_VERSION:
                client_major = "3"
            if client_minor not in MINOR_VERSIONS:
                client_minor = "7"

            version = f"{client_major}.{client_minor}"

        if version.count(".") == 2:
            major, minor, _ = version.split(".")
        elif version.count(".") == 1:
            major, minor = version.split(".")
        else:
            raise Exception(
                "Please specify your Python version on the Spark cluster. Accepted formats: Major.Minor or Major.Minor.Patch; ex. '3.7' or '3.7.16"
            )

        if major != MAJOR_VERSION or minor not in MINOR_VERSIONS:
            raise Exception(
                f"The Python version {version} is not supported. Currently, supported versions are 3.7-3.11."
            )

        """
        The Python versions on the Docker image are 3.7.16, 3.8.16, 3.9.16, 3.10.10, and 3.11.2.
        This conditional statement sets the patch number based on the minor version. 
        """
        if minor == "10":
            patch = "10"
        elif minor == "11":
            patch = "2"
        else:
            patch = "16"

        return f"{major}.{minor}.{patch}"

    def _verify_yarn_config(self):
        if self.master == "yarn" and (
            self.core_site_path == "" or self.yarn_site_path == ""
        ):
            raise Exception(
                "Yarn requires core-site.xml and yarn-site.xml files. "
                "Please copy these files from your Spark instance to local, then provide the local path in "
                "core_site_path and yarn_site_path."
            )

    def type(self):
        return "SPARK"

    def config(self):
        core_site = (
            "" if self.core_site_path == "" else open(self.core_site_path, "r").read()
        )
        yarn_site = (
            "" if self.yarn_site_path == "" else open(self.yarn_site_path, "r").read()
        )

        return {
            "Master": self.master,
            "DeployMode": self.deploy_mode,
            "PythonVersion": self.python_version,
            "CoreSite": core_site,
            "YarnSite": yarn_site,
        }


ExecutorCredentials = Union[EMRCredentials, DatabricksCredentials, SparkCredentials]
