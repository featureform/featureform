#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
#  Copyright 2024 FeatureForm Inc.
#

import os
import json
import argparse
from pathlib import Path
from dataclasses import dataclass

import boto3


# Current & Parent Directories
CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))
PARENT_DIR = os.path.dirname(CURRENT_DIR)

# Secrets Directory
SECRETS_DIRECTORY = os.path.join(PARENT_DIR, ".featureform")
FILE_SECRETS_DIRECTORY = os.path.join(SECRETS_DIRECTORY, "credential_files")

# Environment Variable File
FILE_NAME = ".env"
FILE_PATH = os.path.join(PARENT_DIR, FILE_NAME)

# Create secrets directory
Path(FILE_SECRETS_DIRECTORY).mkdir(parents=True, exist_ok=True)

SECRETS_PREFIX = "testing/"

FILE_SECRETS_CONFIG = {
    "testing/BIGQUERY_CREDENTIALS.json": {
        "Filename": "bigquery_credentials.json",
        "EnvName": "BIGQUERY_CREDENTIALS",
    },
    "testing/firebase.json": {
        "Filename": "firebase.json",
        "EnvName": "FIREBASE_CREDENTIALS",
    },
}

FILE_SECRETS = FILE_SECRETS_CONFIG.keys()


@dataclass
class EnvSecret:
    env: str
    value: str

    def store(self, file):
        file.write(f"{self.env}={self.value}\n")


@dataclass
class FileSecret:
    env: str
    content: str
    filename: str = ""

    def store(self, env_file):
        with open(self.filename, "w") as credential_f:
            credential_f.write(self.content)
        env_file.write(f'{self.env}="{self.filename}"\n')


class AWSSecretsClient:
    def __init__(self, file_secrets_directory):
        self._client = boto3.client("secretsmanager")
        self._file_secrets_directory = file_secrets_directory

    def get_secrets(self):
        all_secrets = []
        next_token = ""
        while True:
            try:
                response = self._get_secrets_response(next_token)
            except Exception as e:
                print(f"Exception occurred while getting secrets: {e}")
                raise e

            # Get Secrets
            for s in response["SecretValues"]:
                secrets = self._parse_secret_response(s)

                all_secrets.extend(secrets)

            next_token = response.get("NextToken", "")
            if not next_token:
                break

        return all_secrets

    def _get_secrets_response(self, next_token):
        testing_only_secrets = [
            {
                "Key": "name",
                "Values": [
                    SECRETS_PREFIX,
                ],
            }
        ]
        if not next_token:
            response = self._client.batch_get_secret_value(
                Filters=testing_only_secrets,
            )
        else:
            response = self._client.batch_get_secret_value(
                Filters=testing_only_secrets,
                NextToken=next_token,
            )
        return response

    def _parse_secret_response(self, secret_response):
        secrets = []
        secret_name = secret_response["Name"]
        if secret_name in FILE_SECRETS:
            filename = FILE_SECRETS_CONFIG[secret_name]["Filename"]
            full_file_path = os.path.join(self._file_secrets_directory, filename)

            secret = FileSecret(
                env=FILE_SECRETS_CONFIG[secret_name]["EnvName"],
                filename=full_file_path,
                content=secret_response["SecretString"],
            )
            secrets.append(secret)
        else:
            env_secrets = json.loads(secret_response["SecretString"])
            for env, value in env_secrets.items():
                secret = EnvSecret(
                    env=env,
                    value=value,
                )
                secrets.append(secret)

        return secrets


class SecretManager:
    def __init__(self, secret_client):
        self._client = secret_client

    def get_secrets(self):
        return self._client.get_secrets()

    def store_secrets(self, secrets, filename):
        print(f"Writing secrets to {filename}")
        with open(filename, "w") as f:
            for secret in secrets:
                secret.store(f)

        print(f"Found {len(secrets)} secrets")


def main(arg):
    if arg.update or not os.path.exists(FILE_PATH):
        aws_secret_client = AWSSecretsClient(arg.secret_files_directory)
        sm = SecretManager(aws_secret_client)
        secrets = sm.get_secrets()
        sm.store_secrets(secrets, arg.env_file)
    else:
        print("File already exists. Use --update or make update_secrets to overwrite.")


def parse_args(args=None):
    parser = argparse.ArgumentParser(description="Featureform Secret Manager")
    parser.add_argument(
        "--update",
        action="store_true",
    )

    parser.add_argument(
        "--env_file",
        default=FILE_PATH,
    )

    parser.add_argument(
        "--secret_files_directory",
        default=FILE_SECRETS_DIRECTORY,
    )

    return parser.parse_args(args)


if __name__ == "__main__":
    main(parse_args())
