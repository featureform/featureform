#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
#  Copyright 2024 FeatureForm Inc.
#

import featureform as ff
import inspect
from typing import Union, Optional
from featureform.register import (
    SourceRegistrar,
    SubscriptableTransformation,
)


def test_client_has_columns_method():
    client = ff.Client(host="localhost:7878", insecure=True, dry_run=True)
    assert hasattr(client, "columns")


def test_columns_method_signature():
    sig = inspect.signature(ff.Client.columns)

    params = sig.parameters

    assert (
        params["source"].annotation
        == Union[SourceRegistrar, SubscriptableTransformation, str]
    )
    assert params["variant"].annotation == Optional[str]
    assert params["variant"].default == None
