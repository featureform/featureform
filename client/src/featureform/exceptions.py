
class InvalidTrainingSetFeatureComputationMode(Exception):
    def __init__(self, feature_name, feature_variant, mode='CLIENT_COMPUTED', message=None):
        if message == None:
            message = (
                f"Feature '{feature_name}:{feature_variant}' is of mode '{mode}'. "
                f"Cannot use '{mode}' computation mode for training sets. "
            )

        Exception.__init__(self, message)
