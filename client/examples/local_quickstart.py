#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
#  Copyright 2024 FeatureForm Inc.
#

import featureform as ff
from featureform import local, ScalarType

ff.set_run("quickstart")

transactions = local.register_file(
    name="transactions",
    description="A dataset of fraudulent transactions",
    path="transactions.csv",
)


@local.df_transformation(inputs=["transactions"])
def average_user_transaction(transactions):
    """the average transaction amount for a user"""
    return transactions.groupby("CustomerID")["TransactionAmount"].mean()


user = ff.register_entity("user")
# Register a column from our transformation as a feature
average_user_transaction.register_resources(
    entity=user,
    entity_column="CustomerID",
    features=[
        {
            "name": "avg_transactions",
            "column": "TransactionAmount",
            "type": ScalarType.FLOAT32,
        },
    ],
)
# Register label from our base Transactions table
transactions.register_resources(
    entity=user,
    entity_column="CustomerID",
    labels=[
        {
            "name": "fraudulent",
            "column": "IsFraud",
            "type": ScalarType.BOOL,
        },
    ],
)

ff.register_training_set(
    "fraud_training",
    label="fraudulent",
    features=["avg_transactions"],
)
