#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
#  Copyright 2024 FeatureForm Inc.
#

from behave import *


@then('An exception that "{match}" "{exception}" should be raised')
def step_impl(context, match, exception):
    if context.text is not None:
        exception = context.text
    if exception == "None":
        assert context.exception is None, f"Exception is {context.exception} not None"
    else:
        if match == "matches":
            assert (
                str(context.exception) == exception
            ), f"\nExpected exception: \n{exception}\nGot: \n{context.exception}"
        elif match == "contains":
            assert exception in str(
                context.exception
            ), f"\nExpected exception: \n{exception}\nGot: \n{context.exception}"
        else:
            raise Exception(f"Unknown match type: {match}")
