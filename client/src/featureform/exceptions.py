from .enums import ComputationMode


class InvalidTrainingSetFeatureComputationMode(Exception):
    def __init__(
        self,
        feature_name,
        feature_variant,
        mode=ComputationMode.CLIENT_COMPUTED.value,
        message=None,
    ):
        if message is None:
            message = (
                f"Feature '{feature_name}:{feature_variant}' is on demand. "
                f"Cannot use {mode} features for training sets. "
            )

        Exception.__init__(self, message)


class FeatureNotFound(Exception):
    def __init__(self, feature_name, feature_variant, message=None):
        error_message = f"Feature '{feature_name}:{feature_variant}' not found. Verify that the feature is registered."

        if message is not None:
            error_message = f"{error_message} {message}"

        Exception.__init__(self, error_message)


class LabelNotFound(Exception):
    def __init__(self, label_name, label_variant, message=None):
        error_message = f"Label '{label_name}:{label_variant}' not found. Verify that the label is registered."
        if message is not None:
            error_message = f"{error_message} {message}"

        Exception.__init__(self, error_message)


class InvalidSQLQuery(Exception):
    def __init__(self, query, message=None):
        error_message = f"Invalid SQL query. Query: ' {query} '"
        if message is not None:
            error_message = f"{error_message} {message}"

        Exception.__init__(self, error_message)
