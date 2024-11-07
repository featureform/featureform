#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
#  Copyright 2024 FeatureForm Inc.
#

from featureform import Client

serving = Client("localhost:443")

dataset = serving.training_set("fraud_training", "default")
training_dataset = dataset
for i, batch in enumerate(training_dataset):
    print(batch.features(), batch.label())
    if i > 25:
        break


user_feat = serving.features([("avg_transaction", "quickstart")], {"user": "C1214240"})
print("\nUser Result: ")
print(user_feat)
