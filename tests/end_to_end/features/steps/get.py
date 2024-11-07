#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
#  Copyright 2024 FeatureForm Inc.
#

from behave import *


@when('I get the provider with name "{name}"')
def step_impl(context, name):
    context.filestore_name = name
    context.filestore = context.client.get_provider(name)
