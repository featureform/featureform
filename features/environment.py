"""
supported methods:
    before_step(context, step), after_step(context, step)
        These run before and after every step.
        The step passed in is an instance of Step.
    before_scenario(context, scenario), after_scenario(context, scenario)
        These run before and after each scenario is run.
        The scenario passed in is an instance of Scenario.
    before_feature(context, feature), after_feature(context, feature)
        These run before and after each feature file is exercised.
        The feature passed in is an instance of Feature.
    before_tag(context, tag), after_tag(context, tag)
"""

import os
import random
import string

from dotenv import load_dotenv

import featureform as ff 

def before_all(context):
    load_dotenv("/Users/kempytoor/Documents/Featureform/e2e/featureform/.env")

    redis_provider = os.getenv("REDIS_PROVIDER_NAME")
    context.redis = ff.get_redis(redis_provider)
    context.name_suffix = get_random_string()
    
    print(f"Starting tests for {context.name_suffix}")

def get_random_string():
    return "".join(random.choice(string.ascii_lowercase) for _ in range(10))

