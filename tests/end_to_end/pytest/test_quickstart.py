#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
#  Copyright 2024 FeatureForm Inc.
#

import numpy as np
import os

from contextlib import redirect_stdout
from featureform.cli import cli


def test_quickstart(ff_client):
    os.environ['FEATUREFORM_HOST'] = 'localhost:7878'

    # Call into Featureform as you would from the CLI.
    cli.main(
        args=['apply', '../../../quickstart/definitions.py', '--insecure', '--verbose'],
        standalone_mode=False
    )

    # Make sure that the provided quickstart files don't throw an exception.
    # There's a lot of output written by these files, which are unnecessary in the test logs,
    # so we ignore stdout.
    with redirect_stdout(open(os.devnull, 'w')):
        with open('../../../quickstart/serving.py') as f:
            exec(f.read())
        with open('../../../quickstart/training.py') as f:
            exec(f.read())

    # Separately test features and training sets.
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