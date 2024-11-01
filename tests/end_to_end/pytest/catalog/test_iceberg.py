#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
#  Copyright 2024 FeatureForm Inc.
#

import pprint

import pytest
from pyiceberg.transforms import BucketTransform, DayTransform

import featureform as ff

# The markers can be used to identify to run a subset of tests
# pytest -m iceberg
pytestmark = [pytest.mark.iceberg]


def test_iceberg_sql_transformation(
    ff_client, emr_fixture, iceberg_transactions_dataset
):
    @emr_fixture.sql_transformation(inputs=[iceberg_transactions_dataset])
    def iceberg_sql_transformation(trx):
        return "SELECT * FROM {{trx}} limit 100"

    ff_client.apply(asynchronous=False, verbose=True)

    df = ff_client.dataframe(iceberg_sql_transformation, iceberg=True)

    assert len(df) == 100


def test_iceberg_df_transformation(
    ff_client, emr_fixture, iceberg_transactions_dataset
):
    @emr_fixture.df_transformation(inputs=[iceberg_transactions_dataset])
    def iceberg_df_transformation(df):
        return df.limit(100)

    ff_client.apply(asynchronous=False, verbose=True)

    df = ff_client.dataframe(iceberg_df_transformation, iceberg=True)

    assert len(df) == 100
