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
            match="Argument must be a path to a file or URL with a valid schema",
        ):
            runner.invoke(apply, ". --dry-run".split(), catch_exceptions=False)

    def test_invalid_url(self):
        runner = CliRunner()
        with pytest.raises(
            ValueError,
            match="Argument must be a path to a file or URL with a valid schema",
        ):
            runner.invoke(
                apply, "www.something.com --dry-run".split(), catch_exceptions=False
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
