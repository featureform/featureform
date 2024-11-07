#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
#  Copyright 2024 FeatureForm Inc.
#

from behave import *
import featureform as ff


@when("I register an ondemand feature")
def step_impl(context):
    @ff.ondemand_feature
    def test_feature(client, params, entity):
        return 1

    @ff.ondemand_feature
    def test_feature2(client, params, entity):
        return 1

    context.ondemand_feature = test_feature2
    context.client.apply()


@then("I can pull the ondemand feature")
def step_impl(context):
    values = context.client.features(
        [("test_feature", ff.get_run()), context.ondemand_feature], {}
    )
    assert values[0] == [1]
    assert values[1] == [1]
