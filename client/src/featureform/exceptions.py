
class InvalidTrainingSetFeatureType(Exception):
    def __init__(self, feature_name, feature_variant, type='ON_DEMAND_CLIENT', message=None):
        if message == None:
            message = (
                f"Feature '{feature_name}:{feature_variant}' is of type '{type}'. "
                f"Cannot use '{type}' feature variant type for training sets. "
            )

        Exception.__init__(self, message)
