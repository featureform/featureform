import sys

import grpc


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


class GrpcClient:
    def __init__(self, wrapped, debug=False, insecure=False, host=None):
        self._insecure = insecure
        self._host = host
        self.wrapped = wrapped
        self.debug = debug
        # Store metadata as an instance variable

    def streaming_wrapper(self, multi_threaded_rendezvous):
        try:
            for message in multi_threaded_rendezvous:
                yield message
        except grpc.RpcError as e:
            # Handle the error gracefully here.
            self.handle_grpc_error(e)

    def __getattr__(self, name):
        attr = getattr(self.wrapped, name)

        def wrapper(*args, **kwargs):
            try:
                # Use the stored metadata for the call
                result = attr(*args, **kwargs)
                # If result is a streaming call, wrap it.
                if self.is_streaming_response(result):
                    return self.streaming_wrapper(result)
                return result
            except grpc.RpcError as e:
                self.handle_grpc_error(e)

        return wrapper

    def handle_grpc_error(self, e):
        sys.tracebacklimit = 0
        initial_ex = None
        if self.debug:
            initial_ex = e
            sys.tracebacklimit = None
        if e.code() == grpc.StatusCode.UNAUTHENTICATED:
            raise Exception(
                f"{BCOLORS.FAIL}Authentication failed.{BCOLORS.ENDC}\n"
                "Your access token is no longer valid. Please re-run the previous command to authenticate."
            )

        elif e.code() == grpc.StatusCode.UNAVAILABLE:
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
