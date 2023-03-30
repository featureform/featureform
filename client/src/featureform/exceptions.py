
class InvalidTrainingSetFeatureCategory(Exception):
    def __init__(self, feature_name, feature_variant, category='ON_DEMAND_CLIENT', message=None):
        if message == None:
            message = (
                f"Feature '{feature_name}:{feature_variant}' is of category '{category}'. "
                f"Cannot use '{category}' feature variant category for training sets. "
            )

        Exception.__init__(self, message)
