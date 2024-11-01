#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
#  Copyright 2024 FeatureForm Inc.
#

import os
import featureform as ff


def test_dynamo():
    client = ff.ResourceClient("localhost:8000", False)
    client.register_dynamodb(
        name="test-dynamo",
        description="Test of dynamo-db creation",
        team="featureform",
        access_key=os.getenv("DYNAMO_ACCESS_KEY"),
        secret_key=os.getenv("DYNAMO_SECRET_KEY"),
    )
    client.apply()
