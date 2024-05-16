import os
import uuid

import featureform as ff


def before_scenario(context, scenario):
    if "use_autovariants" in scenario.tags:
        # Set the environment variable for the duration of the scenario
        os.environ["FF_GET_EQUIVALENT_VARIANTS"] = "true"

    context.unique_prefix = str(uuid.uuid4())
    ff.set_variant_prefix(context.unique_prefix)


def after_scenario(context, scenario):
    if "use_autovariants" in scenario.tags:
        if "FF_GET_EQUIVALENT_VARIANTS" in os.environ:
            del os.environ["FF_GET_EQUIVALENT_VARIANTS"]
