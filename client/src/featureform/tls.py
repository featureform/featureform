#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
#  Copyright 2024 FeatureForm Inc.
#

import grpc
import os
import requests
import json

insecure_protocol = "http://"
secure_protocol = "https://"


def insecure_channel(host):
    keep_alive_ms = int(os.getenv("FEATUREFORM_KEEPALIVE_TIME_MS", "60_000"))
    keep_alive_timeout_ms = int(
        os.getenv("FEATUREFORM_KEEPALIVE_TIMEOUT_MS", "300_000")
    )
    channel_options = [
        ("grpc.enable_http_proxy", 0),
        ("grpc.keepalive_time_ms", keep_alive_ms),
        ("grpc.keepalive_timeout_ms", keep_alive_timeout_ms),
        ("grpc.http2.max_pings_without_data", 100),
        ("grpc.keepalive_permit_without_calls", 1),
    ]
    return grpc.insecure_channel(host, options=channel_options)


def secure_channel(host, cert_path):
    keep_alive_ms = int(os.getenv("FEATUREFORM_KEEPALIVE_TIME_MS", "60_000"))
    keep_alive_timeout_ms = int(
        os.getenv("FEATUREFORM_KEEPALIVE_TIMEOUT_MS", "300_000")
    )
    channel_options = [
        ("grpc.keepalive_time_ms", keep_alive_ms),
        ("grpc.keepalive_timeout_ms", keep_alive_timeout_ms),
        ("grpc.http2.max_pings_without_data", 100),
        ("grpc.keepalive_permit_without_calls", 1),
    ]
    cert_path = cert_path or os.getenv("FEATUREFORM_CERT")
    if cert_path:
        with open(cert_path, "rb") as f:
            credentials = grpc.ssl_channel_credentials(f.read())
    else:
        credentials = grpc.ssl_channel_credentials()
    channel = grpc.secure_channel(host, credentials, options=channel_options)
    return channel


def fetch_cluster_version(version_url=""):
    requests.packages.urllib3.disable_warnings()
    res = requests.get(url=version_url, verify=False)
    response = json.loads(res.text)
    return response["version"]


def get_version_local():
    local_port = os.getenv("LOCALMODE_DASHBOARD_PORT", 3000)
    version_url = f"localhost:{local_port}/data/version"
    return fetch_cluster_version(f"{insecure_protocol}{version_url}")


def get_version_hosted(host):
    cluster_version = ""
    version_url = f"{host}/data/version"
    if host.__contains__(":443"):
        cluster_version = fetch_cluster_version(f"{secure_protocol}{version_url}")
    else:
        cluster_version = fetch_cluster_version(f"{insecure_protocol}{version_url}")
    return cluster_version
