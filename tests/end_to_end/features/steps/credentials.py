#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
#  Copyright 2024 FeatureForm Inc.
#

from behave import *


@then('An exception "{exception}" should be raised')
def step_impl(context, exception):
    if exception == "None":
        assert context.exception is None, f"Exception is {context.exception} not None"
    else:
        assert (
            str(context.exception) == exception
        ), f"Exception is: \n{context.exception} not \n{exception}"
