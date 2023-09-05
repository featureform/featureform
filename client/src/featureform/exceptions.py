import grpc

from .enums import ComputationMode
import sys


class BCOLORS:
    HEADER = "\033[95m"
    OKBLUE = "\033[94m"
    OKCYAN = "\033[96m"
    OKGREEN = "\033[92m"
    WARNING = "\033[93m"
    FAIL = "\033[91m"
    ENDC = "\033[0m"
    BOLD = "\033[1m"
    UNDERLINE = "\033[4m"


class StubExceptionWrapper:
    def __init__(self, wrapped, debug=False):
        self.wrapped = wrapped
        self.debug = debug

    def __getattr__(self, name):
        attr = getattr(self.wrapped, name)

        if callable(attr):

            def wrapper(*args, **kwargs):
                try:
                    return attr(*args, **kwargs)
                except grpc.RpcError as e:
                    sys.tracebacklimit = 0
                    initial_ex = None
                    if self.debug:
                        initial_ex = e
                        sys.tracebacklimit = None
                    if e.code() == grpc.StatusCode.UNAVAILABLE:
                        print("\n")
                        raise Exception(
                            f"{BCOLORS.FAIL}Could not connect to Featureform.{BCOLORS.ENDC}\n"
                            "Please check if your FEATUREFORM_HOST and FEATUREFORM_CERT environment variables are set "
                            "correctly or are explicitly set in the client or command line.\n"
                            f"Details: {e.details()}"
                        ) from initial_ex
                    elif e.code() == grpc.StatusCode.UNKNOWN:
                        raise Exception(
                            f"{BCOLORS.FAIL}Error: {e.details()}{BCOLORS.ENDC}"
                        ) from initial_ex
                    else:
                        raise e

            return wrapper
        else:
            return attr


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


class FeatureNotFound(Exception):
    def __init__(self, feature_name, feature_variant, message=None):
        error_message = f"Feature '{feature_name}:{feature_variant}' not found. Verify that the feature is registered."

        if message != None:
            error_message = f"{error_message} {message}"

        Exception.__init__(self, error_message)


class LabelNotFound(Exception):
    def __init__(self, label_name, label_variant, message=None):
        error_message = f"Label '{label_name}:{label_variant}' not found. Verify that the label is registered."
        if message != None:
            error_message = f"{error_message} {message}"

        Exception.__init__(self, error_message)
