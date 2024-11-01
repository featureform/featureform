#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
#  Copyright 2024 FeatureForm Inc.
#

import sys

import pytest
from click.testing import CliRunner

sys.path.insert(0, "client/src/")
from featureform.cli import apply, version


class TestApply:
    def test_invalid_empty(self):
        runner = CliRunner()
        result = runner.invoke(apply)
        assert result.exit_code == 2

    def test_invalid_path(self):
        runner = CliRunner()
        with pytest.raises(
            ValueError,
            match="Argument must be a path to a file, directory or URL with a valid schema",
        ):
            runner.invoke(
                apply, "nonexistent_file.py --dry-run".split(), catch_exceptions=False
            )

    def test_invalid_url(self):
        runner = CliRunner()
        with pytest.raises(
            ValueError,
            match="Argument must be a path to a file, directory or URL with a valid schema",
        ):
            runner.invoke(
                apply, "www.something.com --dry-run".split(), catch_exceptions=False
            )

    def test_invalid_dir(self):
        runner = CliRunner()
        with pytest.raises(
            ValueError,
            match="Argument must be a path to a file, directory or URL with a valid schema",
        ):
            runner.invoke(
                apply, "./my_ff_dir --dry-run".split(), catch_exceptions=False
            )

    def test_valid_url(self):
        runner = CliRunner()
        result = runner.invoke(
            apply,
            "https://featureform-demo-files.s3.amazonaws.com/quickstart_v2.py --dry-run".split(),
            catch_exceptions=False,
        )
        assert result.exit_code == 0

    def test_valid_file(self):
        runner = CliRunner()
        result = runner.invoke(
            apply,
            "client/examples/quickstart.py --dry-run".split(),
            catch_exceptions=False,
        )
        assert result.exit_code == 0

    def test_valid_dir(self):
        runner = CliRunner()
        result = runner.invoke(
            apply,
            "client/examples/example_dir --dry-run".split(),
            catch_exceptions=False,
        )
        assert result.exit_code == 0

    def test_multiple_values(self):
        runner = CliRunner()
        result = runner.invoke(
            apply,
            "client/examples/quickstart.py https://featureform-demo-files.s3.amazonaws.com/quickstart_v2.py --dry-run".split(),
            catch_exceptions=False,
        )
        assert result.exit_code == 0

    def test_hosted_version(self):
        runner = CliRunner()
        result = runner.invoke(version)
        assert result.exit_code == 0
        assert "Client Version:" in result.output
        assert "Cluster Version:" in result.output
