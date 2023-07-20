"""
gRPC client for FeatureForm service
"""
import logging
import os

import grpc


class ResourceExistsError(Exception):
    pass


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

                    if e.code() == grpc.StatusCode.ALREADY_EXISTS:
                        raise ResourceExistsError()

                    logging.debug(
                        f"gRPC error occurred using stub {name}. Code: {e.code()}, Details: {e.details()}",
                        exc_info=True
                    )

                    message = (
                        f"gRPC error occurred using stub: {name}.\n"
                        f"Code: {e.code()}\n"
                        f"Details: {e.details()}"
                    )

                    raise Exception(message) from None

            return wrapper

        # If the attribute isn't in our stub, fall back to the default behavior
        return self.__getattribute__(name)
