#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
#  Copyright 2024 FeatureForm Inc.
#

import featureform as ff

client = ff.Client(local=True)

training_set = client.training_set("fraud_training", "courageous_jones")
i = 0
for r in training_set:
    print(r)
    i += 1
    if i > 10:
        break

client = ff.Client(local=True)
fpf = client.features([("avg_transactions", "quickstart")], {"user": "C1010012"})
print(fpf)
