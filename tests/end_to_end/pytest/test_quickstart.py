#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
#  Copyright 2024 FeatureForm Inc.
#

import numpy as np

def test_quickstart(ff_client, docker_quickstart_deployment):
    assert docker_quickstart_deployment.start()

    # Exec the definitions file downloaded by the docker_quickstart_deployment.
    with open("./quickstart/definitions.py", "r") as file:
        code = compile(file.read(), "definitions.py", "exec")
        file_globals = {}
        exec(code, file_globals)

    ff_client.apply()

    feature_value = ff_client.features(
        [("avg_transactions", "quickstart")],
        {"user": "C1214240"}
    )
    np.testing.assert_allclose(feature_value, [319.0])

    dataset = ff_client.training_set(
        "fraud_training",
        "quickstart"
    )
    # Just confirm that there are some values being returned.
    # If the enumerator is empty, it will raise a StopIteration exception.
    next(dataset)

    assert docker_quickstart_deployment.stop()