#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
#  Copyright 2024 FeatureForm Inc.
#

import os
from dotenv import load_dotenv
from datetime import datetime


def before_all(context):
    # find the path to the .env file
    # it will be in the root of the project which is three levels up
    env_path = os.path.join(os.path.dirname(__file__), "../../../.env")

    # Load environment variables from .env file
    load_dotenv(env_path)


def before_step(context, step):
    step_id = f"{context.scenario.name}:{step.name}"
    context.step_start = datetime.now()
    print(f"Starting {step_id}")


def after_step(context, step):
    step_id = f"{context.scenario.name}:{step.name}"
    delta = datetime.now() - context.step_start
    print(
        f"Finished {step_id} with status {step.status} in {delta.total_seconds()} seconds"
    )
