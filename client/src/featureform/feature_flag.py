import os


def is_enabled(feature_name, default=False):
    """
    Returns True if the feature is enabled, currently using an environment variable.
    Interprets the environment variable as a boolean if it's a valid representation, else defaults to the specified default.
    """
    value = os.getenv(feature_name)
    if value is not None:
        return value.lower() == "true"
    return default
