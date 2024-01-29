import sys

import grpc

from google.rpc import error_details_pb2, status_pb2
from google.protobuf import any_pb2


class GrpcClient:
    def __init__(self, wrapped, debug=False, insecure=False, host=None):
        self.wrapped = wrapped
        self._insecure = insecure
        self._host = host
        self.debug = debug
        # Store metadata as an instance variable

    def streaming_wrapper(self, multi_threaded_rendezvous):
        try:
            for message in multi_threaded_rendezvous:
                yield message
        except grpc.RpcError as e:
            # Handle the error gracefully here.
            self.handle_grpc_error(e)

    @staticmethod
    def is_streaming_response(obj):
        return hasattr(obj, "__iter__") and not isinstance(
            obj, (str, bytes, dict, list)
        )

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
        initial_ex = None

        if self.debug:
            initial_ex = e
            sys.tracebacklimit = None

        # get details from the error
        if e.code() == grpc.StatusCode.INTERNAL:
            status_proto = status_pb2.Status()
            status_proto.MergeFromString(e.trailing_metadata()[0].value)

            for detail in status_proto.details:
                any_msg = any_pb2.Any()
                any_msg.CopyFrom(detail)
                if any_msg.Is(error_details_pb2.ErrorInfo.DESCRIPTOR):
                    error_info = error_details_pb2.ErrorInfo()
                    any_msg.Unpack(error_info)
                    reason = error_info.reason
                    metadata = error_info.metadata

                    # log reason and metadata
            raise Exception(f"{e.details()}\n")
        elif e.code() == grpc.StatusCode.UNAVAILABLE:
            print("\n")
            raise Exception(
                f"Could not connect to Featureform.\n"
                "Please check if your FEATUREFORM_HOST and FEATUREFORM_CERT environment variables are set "
                "correctly or are explicitly set in the client or command line.\n"
                f"Details: {e.details()}"
            ) from initial_ex
        elif e.code() == grpc.StatusCode.UNKNOWN:
            raise Exception(f"Error: {e.details()}") from initial_ex
        else:
            raise e
