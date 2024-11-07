#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
#  Copyright 2024 FeatureForm Inc.
#

from behave import *
from featureform import register_spark
import featureform as ff
import os


@when("I create a user")
def step_impl(context):
    context.exception = None
    try:
        ff.register_user("test_user")
        context.client.apply()
    except Exception as e:
        context.exception = e
