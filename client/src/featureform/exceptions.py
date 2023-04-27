from .enums import ComputationMode


class InvalidTrainingSetFeatureComputationMode(Exception):
    def __init__(
        self,
        feature_name,
        feature_variant,
        mode=ComputationMode.CLIENT_COMPUTED.value,
        message=None,
    ):
        if message == None:
            message = (
                f"Feature '{feature_name}:{feature_variant}' is on demand. "
                f"Cannot use {mode} features for training sets. "
            )

        Exception.__init__(self, message)
