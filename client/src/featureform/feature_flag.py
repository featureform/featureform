import os


def is_enabled(feature_name, default=False):
    """
    Returns True if the feature is enabled, currently using an environment variable
    """
    value = os.getenv(feature_name).lower()
    if value == "true":
        return True
    return default
