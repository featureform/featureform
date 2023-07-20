"""
gRPC client for FeatureForm service
"""
import logging

import grpc


class GrpcStub:
    def __init__(self, stub):
        self._stub = stub

    # TODO add retry logic here
    def __getattr__(self, name):
        # If the attribute exists in our stub, we want to intercept it
        if hasattr(self._stub, name):

            def wrapper(*args, **kwargs):
                try:
                    # Call the method from our stub
                    return getattr(self._stub, name)(*args, **kwargs)
                except grpc.RpcError as e:
                    # Handle the error however you want
                    logging.debug(
                        f"gRPC error occurred in {name}. Code: {e.code()}, Details: {e.details()}"
                    )
                    raise Exception(
                        f"gRPC error occurred in {name}. Code: {e.code()}, Details: {e.details()}"
                    ) from None

            return wrapper

        # If the attribute isn't in our stub, fall back to the default behavior
        return self.__getattribute__(name)
