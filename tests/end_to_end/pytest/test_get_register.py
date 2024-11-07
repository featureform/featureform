#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
#  Copyright 2024 FeatureForm Inc.
#

import pytest
import featureform as ff


@pytest.mark.postgres
def test_latest_variant(ff_client, redis_fixture, postgres_transactions_dataset):
    client = ff_client
    redis = redis_fixture
    transactions = postgres_transactions_dataset

    @ff.entity
    class User:
        table2_multi_variants = ff.Variants(
            {
                "default": ff.Feature(
                    transactions[["customerid", "custlocation", "timestamp"]],
                    type=ff.String,
                    variant="default",
                    inference_store=redis,
                ),
                "variant2": ff.Feature(
                    transactions[["customerid", "isfraud", "timestamp"]],
                    type=ff.Bool,
                    variant="variant2",
                    inference_store=redis,
                ),
            }
        )

    client.apply(asynchronous=False, verbose=True)
    latest_variant = client.latest_variant(
        User.table2_multi_variants, ff.ResourceType.FEATURE_VARIANT
    )
    assert latest_variant == "variant2"

    @ff.entity
    class SameUser:
        table2_multi_variants = ff.Feature(
            transactions[["customerid", "custaccountbalance", "timestamp"]],
            type=ff.String,
            variant="variant3",
            inference_store=redis,
        )

    client.apply(asynchronous=False, verbose=True)
    next_latest_variant = client.latest_variant(
        "table2_multi_variants", ff.ResourceType.FEATURE_VARIANT
    )
    assert next_latest_variant == "variant3"
