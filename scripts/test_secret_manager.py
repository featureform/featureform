#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
#  Copyright 2024 FeatureForm Inc.
#

import os
import tempfile
from pathlib import Path

from secret_manager import EnvSecret, FileSecret, SecretManager


class MockSecretClient:
    def __init__(self, test_directory):
        self._secrets = [
            EnvSecret(
                "TEST_KEY",
                "TEST_VALUE",
            ),
            FileSecret(
                "BIGQUERY_CREDENTIALS",
                "test_secrets",
                os.path.join(test_directory, "bigquery_credentials.json"),
            ),
        ]

    def get_secrets(self):
        return self._secrets


def test_get_secrets():
    with tempfile.TemporaryDirectory() as temp_directory:
        mock_secret_client = MockSecretClient(temp_directory)
        sm = SecretManager(mock_secret_client)
        secrets = sm.get_secrets()

        assert len(secrets) == 2


def test_store_secrets():
    with tempfile.TemporaryDirectory() as temp_directory:
        env_file = os.path.join(temp_directory, ".env")

        mock_secret_client = MockSecretClient(temp_directory)
        sm = SecretManager(mock_secret_client)
        secrets = sm.get_secrets()
        sm.store_secrets(secrets, env_file)

        with open(env_file, "r") as f:
            num_lines = 0
            for _ in f.readlines():
                num_lines += 1

            assert (
                num_lines == 2
            ), f"didn't get the same number of environment variables as expected. got {num_lines}; expected: 2"
