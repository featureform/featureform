#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
#  Copyright 2024 FeatureForm Inc.
#

import os

from behave import *


@given("Featureform is installed")
def step_impl(context):
    import featureform


@when('I create a "{mode}" "{secure}" client for "{host}"')
def step_impl(context, mode, secure, host):
    import featureform

    if secure == "secure":
        is_secure = True
    elif secure == "insecure":
        is_secure = False
    elif secure == "None":
        is_secure = None
    else:
        raise ValueError(
            f"Invalid secure value: {secure}. Must be 'secure', 'insecure', or 'None'"
        )

    if mode == "localmode":
        context.client = featureform.Client(localmode=True)
    elif mode == "hosted":
        context.client = featureform.Client(host=host, insecure=not is_secure)
    else:
        raise ValueError(f"Invalid mode value: {mode}. Must be 'localmode' or 'hosted'")
