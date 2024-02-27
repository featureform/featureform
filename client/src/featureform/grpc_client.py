import contextlib
import logging
import sys

import grpc

from google.rpc import error_details_pb2, status_pb2
from google.protobuf import any_pb2

logging.basicConfig(level=logging.DEBUG, filename="grpc_debug.log")


class GrpcClient:
    def __init__(self, wrapped, debug=False, insecure=False, host=None):
        self.wrapped = wrapped
        self._insecure = insecure
        self._host = host
        self.debug = debug
        self.expected_codes = [
            grpc.StatusCode.INTERNAL,
            grpc.StatusCode.NOT_FOUND,
            grpc.StatusCode.ALREADY_EXISTS,
            grpc.StatusCode.INVALID_ARGUMENT,
        ]

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
        traceback_limit = 0
        initial_ex = None

        if self.debug:
            initial_ex = e
            traceback_limit = None

        with limited_traceback(traceback_limit):
            # get details from the error
            if e.code() in self.expected_codes:
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

                        logging.debug(
                            f"Error: {status_proto.message}: {reason}\n{_format_metadata(metadata)}"
                        )
                raise Exception(
                    f"{reason}: {status_proto.message}\n{_format_metadata(metadata)}"
                ) from None
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
                print("RAISING ERROR")
            raise e


@contextlib.contextmanager
def limited_traceback(limit):
    original_limit = getattr(sys, "tracebacklimit", None)
    sys.tracebacklimit = limit
    try:
        yield
    finally:
        sys.tracebacklimit = original_limit


def _format_metadata(metadata):
    return "\n".join([f"{k}: {v}" for k, v in metadata.items()])
