#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
#  Copyright 2025 FeatureForm Inc.
#

from iceberg_streamer import StreamerService, port
import pytest
import threading
import time
import pyarrow.flight as fl

@pytest.fixture(scope="session")
def streamer_service():
    server = StreamerService()
    server_thread = threading.Thread(target=server.serve, daemon=True)
    server_thread.start()
    time.sleep(1)
    yield server
    server.shutdown()
    server_thread.join()

@pytest.fixture(scope="session")
def streamer_client(streamer_service):
    return fl.connect(f"grpc://localhost:{port}")
