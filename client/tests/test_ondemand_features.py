#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
#  Copyright 2024 FeatureForm Inc.
#

import dill
import time

import pytest
import numpy as np

import featureform as ff
from featureform.resources import ResourceStatus, OnDemandFeatureVariant


@pytest.fixture(autouse=True)
def before_and_after_each(setup_teardown):
    setup_teardown()
    yield
    setup_teardown()


@pytest.mark.local
def test_ondemand_feature_decorator_class():
    name = "test_ondemand_feature"
    owner = "ff_tester"

    decorator = OnDemandFeatureVariant(owner=owner, name=name, variant="default")
    decorator_2 = OnDemandFeatureVariant(owner=owner, name=name, variant="default")

    assert decorator.name_variant() == (name, "default")
    assert decorator.get_resource_type() == ff.ResourceType.ONDEMAND_FEATURE
    assert decorator.get_status() == ResourceStatus.READY
    assert decorator.is_ready() is True
    assert decorator == decorator_2


@pytest.mark.local
def test_ondemand_decorator():
    owner = "ff_tester"

    @OnDemandFeatureVariant(owner=owner, variant="default")
    def test_fn():
        return 1 + 1

    expected_query = dill.dumps(test_fn.__code__)

    assert test_fn.name_variant() == (test_fn.__name__, "default")
    assert test_fn.query == expected_query
