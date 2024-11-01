#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
#  Copyright 2024 FeatureForm Inc.
#

import pytest
from featureform.deploy import (
    Deployment,
)


@pytest.mark.parametrize(
    "quickstart",
    [
        (True),
        (False),
    ],
)
def test_deployment_class(quickstart):
    deployment = Deployment(quickstart)
    assert deployment is not None
    assert deployment._quickstart == quickstart
    assert deployment.start() is None
    assert deployment.stop() is None
    assert deployment.status is None
    assert deployment.config == []


@pytest.mark.parametrize(
    "deploy, expected_config",
    [
        ("docker_deployment", "docker_deployment_config"),
        ("docker_quickstart_deployment", "docker_quickstart_deployment_config"),
    ],
)
def test_deployment_config(deploy, expected_config, request, mocker):
    mocker.patch("docker.from_env")

    deployment = request.getfixturevalue(deploy)
    expected_config = request.getfixturevalue(expected_config)
    config = deployment.config

    assert len(config) == len(expected_config)

    for c, expected_c in zip(config, expected_config):
        for name, value in c._asdict().items():
            assert value == getattr(expected_c, name)


@pytest.mark.parametrize(
    "deployment, expected_status",
    [
        ("docker_deployment", "docker_deployment_status"),
    ],
)
def test_deployment_status(deployment, expected_status, request):
    deployment = request.getfixturevalue(deployment)
    expected_status = request.getfixturevalue(expected_status)
    status = deployment.status

    assert status == expected_status


@pytest.mark.parametrize(
    "deployment, expected_failure",
    [
        ("docker_deployment", False),
        ("docker_quickstart_deployment", False),
    ],
)
def test_deployment(deployment, expected_failure, request):
    d = request.getfixturevalue(deployment)
    assert d.start() == (not expected_failure)
    assert d.stop() == (not expected_failure)
